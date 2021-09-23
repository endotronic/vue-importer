import argparse
from datetime import datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple, Union

import attr
import yaml
from pyemvue import PyEmVue  # type: ignore
from pyemvue.enums import Scale, Unit  # type: ignore


class Config:
    def __init__(self, path: str) -> None:
        with open(path, "r") as config_file:
            self.config_dict = yaml.load(config_file, Loader=yaml.Loader)
            self.locations = dict()  # type: Dict[str, "Config.ConfigLocation"]
            self.all_names = set()  # type: Set[str]

            if "locations" in self.config_dict:
                for location_name, location_dict in self.config_dict[
                    "locations"
                ].items():
                    if location_name in self.all_names:
                        raise Exception(
                            "Found location named {} defined multiple times in config - this is not allowed".format(
                                location_name
                            )
                        )
                    self.all_names.add(location_name)

                    circuits_dict = dict()
                    if "circuits" in location_dict:
                        circuits_dict.update(
                            self._read_circuits_from_config_dict(
                                location_dict["circuits"], are_outlets=False
                            )
                        )
                    if "outlets" in location_dict:
                        circuits_dict.update(
                            self._read_circuits_from_config_dict(
                                location_dict["outlets"], are_outlets=True
                            )
                        )

                    self.locations[location_name] = Config.ConfigLocation(
                        name=location_name, circuits=circuits_dict
                    )

    def _read_circuits_from_config_dict(
        self,
        config_dict: Union[Dict[str, Any], List[str]],
        are_outlets: bool,
    ) -> Dict[str, "Config.ConfigCircuit"]:
        circuits = dict()  # type:  Dict[str, "Config.ConfigCircuit"]

        if isinstance(config_dict, list):
            for circuit_name in config_dict:
                circuits[circuit_name] = Config.ConfigCircuit(
                    name=circuit_name,
                    display_name=circuit_name,
                    label=None,
                    is_outlet=are_outlets,
                    remainder_name=None,
                    child_circuits=dict(),
                )

        elif isinstance(config_dict, dict):
            for circuit_name, circuit_dict in config_dict.items():
                if circuit_name in self.all_names:
                    raise Exception(
                        "Found circuit named {} defined multiple times in config - this is not allowed".format(
                            circuit_name
                        )
                    )
                self.all_names.add(circuit_name)

                child_circuits = dict()
                if "circuits" in circuit_dict:
                    child_circuits.update(
                        self._read_circuits_from_config_dict(
                            config_dict=circuit_dict["circuits"],
                            are_outlets=False,
                        )
                    )
                if "outlets" in circuit_dict:
                    child_circuits.update(
                        self._read_circuits_from_config_dict(
                            config_dict=circuit_dict["outlets"],
                            are_outlets=True,
                        )
                    )

                display_name = circuit_name
                if "display_name" in circuit_dict:
                    display_name = circuit_dict["display_name"]

                label = None
                if "label" in circuit_dict:
                    label = circuit_dict["label"]

                remainder_name = None
                if "remainder" in circuit_dict:
                    remainder_name = circuit_dict["remainder"]

                circuits[circuit_name] = Config.ConfigCircuit(
                    name=circuit_name,
                    display_name=display_name,
                    label=label,
                    is_outlet=are_outlets,
                    remainder_name=remainder_name,
                    child_circuits=child_circuits,
                )

        return circuits

    @property
    def accounts(self) -> Any:
        return self.config_dict["accounts"]

    @attr.s
    class ConfigLocation:
        name = attr.ib(type=str)
        circuits = attr.ib(type=Dict[str, "Config.ConfigCircuit"])

    @attr.s
    class ConfigCircuit:
        name = attr.ib(type=str)
        display_name = attr.ib(type=str)
        label = attr.ib(type=Optional[str])
        is_outlet = attr.ib(type=bool)
        remainder_name = attr.ib(type=Optional[str])
        child_circuits = attr.ib(type=Dict[str, "Config.ConfigCircuit"])


@attr.s
class Location:
    name = attr.ib(type=str)
    circuits = attr.ib(type=Dict[str, "Circuit"])


@attr.s
class Circuit:
    name = attr.ib(type=str)
    account_name = attr.ib(type=str)
    device_gid = attr.ib(type=str)
    channel_num = attr.ib(type=str)
    is_outlet = attr.ib(type=bool)
    child_circuits = attr.ib(type=Dict[str, "Circuit"])
    display_name = attr.ib(type=Optional[str], default=None)
    label = attr.ib(type=Optional[str], default=None)
    remainder_name = attr.ib(type=Optional[str], default=None)


class Emporia:
    def __init__(self, config: Config) -> None:
        self.config = config
        self.accounts = None  # type: Optional[Dict[str, PyEmVue]]
        self.locations = None  # type: Optional[Dict[str, Location]]

    def do_logins_and_build_circuits(self) -> None:
        if self.accounts:
            return

        self.accounts = dict()
        self.locations = {
            name: Location(name=name, circuits=dict())
            for name in self.config.locations.keys()
        }

        circuits_to_add = dict()  # type: Dict[str, Circuit]
        for account_name, account_details in self.config.accounts.items():
            self.accounts[account_name] = account = PyEmVue()
            account.login(
                username=account_details["email"], password=account_details["password"]
            )

            for device in account.get_devices():
                if len(device.channels) == 1:
                    circuits_to_add[device.device_name] = Circuit(
                        name=device.device_name,
                        account_name=account_name,
                        device_gid=device.device_gid,
                        channel_num=device.channels[0].channel_num,
                        is_outlet=bool(device.outlet),
                        child_circuits=dict(),
                    )
                else:
                    for channel in device.channels:
                        circuits_to_add[channel.name] = Circuit(
                            name=channel.name,
                            account_name=account_name,
                            device_gid=device.device_gid,
                            channel_num=channel.channel_num,
                            is_outlet=False,
                            child_circuits=dict(),
                        )

        for config_location in self.config.locations.values():
            self._populate_circuits_recursive(
                config_circuits=config_location.circuits.values(),
                circuit_container=self.locations[config_location.name].circuits,
                circuits_to_add=circuits_to_add,
            )

        # Put remaining (unconfigured) circuits in default locations
        for circuit in circuits_to_add.values():
            location = self.config.accounts[circuit.account_name].get(
                "location", circuit.account_name
            )
            if location not in self.locations:
                self.locations[location] = Location(name=location, circuits=dict())

            self.locations[location].circuits[circuit.name] = circuit

    def _populate_circuits_recursive(
        self,
        config_circuits: Iterable[Config.ConfigCircuit],
        circuit_container: Dict[str, Circuit],
        circuits_to_add: Dict[str, Circuit],
    ) -> None:
        for config_circuit in config_circuits:
            if config_circuit.name in circuits_to_add:
                circuit_container[config_circuit.name] = this_circuit = circuits_to_add[
                    config_circuit.name
                ]
                this_circuit.display_name = (
                    config_circuit.display_name or config_circuit.name
                )
                this_circuit.label = config_circuit.label
                this_circuit.remainder_name = config_circuit.remainder_name

                if config_circuit.is_outlet != this_circuit.is_outlet:
                    print(
                        "WARNING: Circuit {} {} configured as an outlet, but actually {} an outlet.".format(
                            config_circuit.name,
                            "is" if config_circuit.is_outlet else "is not",
                            "is" if this_circuit.is_outlet else "is not",
                        )
                    )

                del circuits_to_add[config_circuit.name]
                self._populate_circuits_recursive(
                    config_circuits=config_circuit.child_circuits.values(),
                    circuit_container=this_circuit.child_circuits,
                    circuits_to_add=circuits_to_add,
                )
            else:
                print(
                    "WARNING: Configured circuit {} not found in actual devices and channels".format(
                        config_circuit.name
                    )
                )


def recursive_print_circuits(circuits: Iterable[Circuit], indent: int = 2) -> None:
    for circuit in circuits:
        outlet_suffix = "(outlet)" if circuit.is_outlet else ""
        label_suffix = "[{}]".format(circuit.label) if circuit.label else ""
        print(
            "{}- {} {}{}".format(
                " " * indent, circuit.display_name, label_suffix, outlet_suffix
            )
        )

        recursive_print_circuits(circuit.child_circuits.values(), indent=indent + 2)
        if circuit.remainder_name:
            print(
                "{}- {} (remainder)".format(
                    " " * (indent + 2),
                    circuit.remainder_name,
                )
            )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Runs a webserver for Prometheus scraping and proxies requests to the Emporia cloud API."
    )
    parser.add_argument(
        "-c", "--config", help="path to config file (yaml)", default="config.yaml"
    )
    parser.add_argument("-e", "--email", help="account email (overrides config)")
    parser.add_argument("-p", "--password", help="account password (overrides config)")
    parser.add_argument(
        "-i", "--interval", help="minimum interval for queries (in seconds)", default=15
    )
    parser.add_argument("-l", "--lag", help="lag for query (in seconds)", default=5)
    parser.add_argument(
        "-d",
        "--list-devices",
        action="store_true",
        help="list available devices/channels and exit",
    )
    args = parser.parse_args()

    config = Config(args.config)
    emporia = Emporia(config)

    if args.list_devices:
        emporia.do_logins_and_build_circuits()
        assert emporia.locations, "Failed to build locations"
        for location in emporia.locations.values():
            print("Location: " + location.name)
            print("Circuits:")
            recursive_print_circuits(location.circuits.values())
    else:
        print("not implemented")