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
            self.circuit_to_parent_mapping = dict()  # type: Dict[str, str]
            self.circuit_names = set()  # type: Set[str]
            all_names = set()  # type: Set[str]

            def _read_circuits_from_config_dict(
                config_dict: Union[Dict[str, Any], List[str]],
                parent_circuit_name: Optional[str] = None,
            ) -> Dict[str, "Config.ConfigCircuit"]:
                circuits = dict()  # type:  Dict[str, "Config.ConfigCircuit"]

                if isinstance(config_dict, list):
                    for circuit_name in config_dict:
                        circuits[circuit_name] = Config.ConfigCircuit(
                            name=circuit_name,
                            child_circuits=dict(),
                        )
                        if parent_circuit_name:
                            self.circuit_to_parent_mapping[
                                circuit_name
                            ] = parent_circuit_name

                elif isinstance(config_dict, dict):
                    for circuit_name, circuit_dict in config_dict.items():
                        if circuit_name in all_names:
                            raise Exception(
                                "Found circuit named {} defined multiple times in config - this is not allowed".format(
                                    circuit_name
                                )
                            )
                        self.circuit_names.add(circuit_name)
                        all_names.add(circuit_name)

                        if parent_circuit_name:
                            self.circuit_to_parent_mapping[
                                circuit_name
                            ] = parent_circuit_name

                        circuits[circuit_name] = Config.ConfigCircuit(
                            name=circuit_name,
                            child_circuits=_read_circuits_from_config_dict(
                                parent_circuit_name=circuit_name,
                                config_dict=circuit_dict,
                            ),
                        )

                return circuits

            if "locations" in self.config_dict:
                for location_name, circuits in self.config_dict["locations"].items():
                    if location_name in all_names:
                        raise Exception(
                            "Found location named {} defined multiple times in config - this is not allowed".format(
                                location_name
                            )
                        )
                    all_names.add(location_name)

                    circuits_dict = _read_circuits_from_config_dict(circuits)
                    self.locations[location_name] = Config.ConfigLocation(
                        name=location_name, circuits=circuits_dict
                    )

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

        for account_name, account_details in self.config.accounts.items():
            self.accounts[account_name] = account = PyEmVue()
            account.login(
                username=account_details["email"], password=account_details["password"]
            )

            found_circuit_names = set()  # type: Set[str]
            devices = account.get_devices()
            for device in devices:
                if len(device.channels) == 1:
                    found_circuit_names.add(device.device_name)
                    assert self._add_circuit(
                        Circuit(
                            name=device.device_name,
                            account_name=account_name,
                            device_gid=device.device_gid,
                            channel_num=device.channels[0].channel_num,
                            is_outlet=bool(device.outlet),
                            child_circuits=dict(),
                        ),
                        default_location=account_name,
                    ), (
                        "Failed to add " + device.device_name
                    )
                else:
                    for channel in device.channels:
                        found_circuit_names.add(channel.name)
                        assert self._add_circuit(
                            Circuit(
                                name=channel.name,
                                account_name=account_name,
                                device_gid=device.device_gid,
                                channel_num=channel.channel_num,
                                is_outlet=False,
                                child_circuits=dict(),
                            ),
                            default_location=account_name,
                        ), (
                            "Failed to add " + device.device_name
                        )

            missing_circuits = self.config.circuit_names - found_circuit_names
            if missing_circuits:
                print("WARNING: Some circuits that were configured were not found.")
                print("These are: " + ",".join(missing_circuits))

    def _add_circuit(self, circuit: Circuit, default_location: str) -> None:
        assert self.locations, "Programming error: missing locations"

        if circuit.name not in self.config.circuit_to_parent_mapping:
            location = self.config.accounts[circuit.account_name].get(
                "location", default_location
            )
            if location not in self.locations:
                self.locations[location] = Location(name=location, circuits=dict())

            self.locations[location].circuits[circuit.name] = circuit
        else:
            parent_circuit_name = self.config.circuit_to_parent_mapping[circuit.name]
            if parent_circuit_name in self.locations:
                self.locations[parent_circuit_name].circuits[circuit.name] = circuit
            else:
                # This is a stupid way to do this, but it probably doesn't matter
                def _add_circuit_recursive(
                    circuit: Circuit, container: Circuit, parent_circuit_name: str
                ) -> bool:
                    if container.name == parent_circuit_name:
                        container.child_circuits[circuit.name] = circuit
                        return True
                    else:
                        for idx_circuit in location.child_circuits.values():
                            if _add_circuit_recursive(
                                circuit, idx_circuit, parent_circuit_name
                            ):
                                return True
                        return False

                done = False
                for location in self.locations.values():
                    for idx_circuit in location.circuits.values():
                        if _add_circuit_recursive(
                            circuit, idx_circuit, parent_circuit_name
                        ):
                            if done:
                                raise Exception(
                                    "Attempted to duplicate circuit {} times".format(
                                        circuit.name
                                    )
                                )
                            done = True


def recursive_print_circuits(circuits: Iterable[Circuit], indent: int = 2) -> None:
    for circuit in circuits:
        outlet_suffix = "(outlet)" if circuit.is_outlet else ""
        print("{}- {} {}".format(" " * indent, circuit.name, outlet_suffix))
        recursive_print_circuits(circuit.child_circuits.values(), indent=indent + 2)


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
