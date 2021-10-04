import argparse
from datetime import datetime, timedelta
import signal
from threading import Event, Lock
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple, Union

import attr
from prometheus_client import start_http_server, Gauge  # type: ignore
from pyemvue import PyEmVue  # type: ignore
from pyemvue.enums import Scale, Unit  # type: ignore
import yaml


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
    location = attr.ib(type=Optional[str], default=None)
    parent_circuit = attr.ib(type=Optional["Circuit"], default=None)
    display_name = attr.ib(type=Optional[str], default=None)
    label = attr.ib(type=Optional[str], default=None)
    remainder_name = attr.ib(type=Optional[str], default=None)


@attr.s
class CachedUsage:
    circuit_usage = attr.ib(type=Dict[str, float])
    fetch_time = attr.ib(type=datetime)


class Emporia:
    def __init__(self, config: Config) -> None:
        self.config = config
        self.accounts = None  # type: Optional[Dict[str, PyEmVue]]
        self.locations = None  # type: Optional[Dict[str, Location]]

        self.circuits_by_name = dict()  # type: Dict[str, Circuit]
        self.circuits_by_device = dict()  # type: Dict[Tuple[str, str], Circuit]

        self.cached_usage = None  # type: Optional[CachedUsage]
        self.cache_lock = Lock()

        self.gauge = Gauge(
            "emporia_usage",
            "Usage of circuit in watts",
            (
                "circuit",
                "account",
                "device_gid",
                "channel_num",
                "emporia_name",
                "location",
                "parent_circuit",
                "contains_circuits",
                "circuit_type",
                "label",
            ),
        )

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

        # Before populating circuits, which will remove them from this
        # convenient dict, build lookup tables so they can be accessed
        # by device_gid and channel_num or by name
        self.circuits_by_name = {
            circuit.name: circuit for circuit in circuits_to_add.values()
        }
        self.circuits_by_device = {
            (circuit.device_gid, circuit.channel_num): circuit
            for circuit in circuits_to_add.values()
        }

        for config_location in self.config.locations.values():
            self._populate_circuits_recursive(
                config_circuits=config_location.circuits.values(),
                circuit_container=self.locations[config_location.name].circuits,
                circuits_to_add=circuits_to_add,
                location=config_location.name,
            )

        # Put remaining (unconfigured) circuits in default locations
        for circuit in circuits_to_add.values():
            location = self.config.accounts[circuit.account_name].get(
                "location", circuit.account_name
            )
            if location not in self.locations:
                self.locations[location] = Location(name=location, circuits=dict())

            self.locations[location].circuits[circuit.name] = circuit
            circuit.location = location

    def _populate_circuits_recursive(
        self,
        config_circuits: Iterable[Config.ConfigCircuit],
        circuit_container: Dict[str, Circuit],
        circuits_to_add: Dict[str, Circuit],
        location: str,
        parent_circuit: Optional[Circuit] = None,
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
                this_circuit.location = location
                this_circuit.parent_circuit = parent_circuit

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
                    location=location,
                    parent_circuit=this_circuit,
                )
            else:
                print(
                    "WARNING: Configured circuit {} not found in actual devices and channels".format(
                        config_circuit.name
                    )
                )

    def get_usage_for_circuits(self) -> Dict[str, float]:
        assert self.accounts, "Programming error, must log in first"
        circuit_usage = dict()  # type: Dict[str, float]

        for account_name, account in self.accounts.items():
            query_time = datetime.utcnow() - timedelta(seconds=5)
            device_gids = [
                circuit.device_gid
                for circuit in self.circuits_by_name.values()
                if circuit.account_name == account_name
            ]

            print('Querying usage for account "{}"'.format(account_name))
            device_usage_dict = account.get_device_list_usage(
                deviceGids=device_gids,
                instant=query_time,
                scale=Scale.SECOND.value,
                unit=Unit.KWH.value,
            )

            for device_gid, device_usage in device_usage_dict.items():
                for channel in device_usage.channels.values():
                    circuit = self.circuits_by_device.get(
                        (device_gid, channel.channel_num)
                    )
                    # Note that there may be extra information, e.g. "Balance"
                    # in the result, and channel.usage may be None, e.g. if
                    # a smart outlet is off.
                    if circuit:
                        if channel.usage:
                            circuit_usage[circuit.name] = channel.usage * 3600 * 1000
                        else:
                            circuit_usage[circuit.name] = 0

        return circuit_usage

    def get_usage_for_circuits_with_cache(self) -> Dict[str, float]:
        with self.cache_lock:
            now = datetime.utcnow()
            cache_ttl = timedelta(seconds=15)
            if self.cached_usage and now - self.cached_usage.fetch_time < cache_ttl:
                return self.cached_usage.circuit_usage

            circuit_usage = self.get_usage_for_circuits()
            self.cached_usage = CachedUsage(
                circuit_usage=circuit_usage, fetch_time=datetime.utcnow()
            )
            return circuit_usage

    def build_gauges(self) -> None:
        for circuit in self.circuits_by_name.values():
            self._build_gauge(circuit)

    def _build_gauge(self, circuit: Circuit) -> None:
        parent_circuit = None
        if circuit.parent_circuit:
            parent_circuit = circuit.parent_circuit.display_name

        circuit_type = "circuit"
        if circuit.is_outlet:
            circuit_type = "outlet"

        contains_circuits = bool(len(circuit.child_circuits))

        labeled_gauge = self.gauge.labels(
            circuit=circuit.display_name,
            account=circuit.account_name,
            device_gid=circuit.device_gid,
            channel_num=circuit.channel_num,
            emporia_name=circuit.name,
            location=circuit.location,
            parent_circuit=parent_circuit,
            contains_circuits=contains_circuits,
            circuit_type=circuit_type,
            label=circuit.label or "",
        )

        def get_usage() -> float:
            usage = self.get_usage_for_circuits_with_cache()

            # Raise an exception if we have no usage and let
            # Prometheus handle the exception to return no value
            assert circuit.name in usage, "No usage for " + circuit.name
            return usage[circuit.name]

        labeled_gauge.set_function(get_usage)

        if contains_circuits:
            # Add a gauge for the remainder (balance)
            remainder_name = circuit.remainder_name or "{} (remainder)".format(
                circuit.display_name
            )

            labeled_gauge = self.gauge.labels(
                circuit=remainder_name,
                account=circuit.account_name,
                device_gid=None,
                channel_num=None,
                emporia_name=None,
                location=circuit.location,
                parent_circuit=circuit.name,
                contains_circuits=False,
                circuit_type="remainder",
                label="",
            )

            def get_remainder() -> float:
                usage = self.get_usage_for_circuits_with_cache()
                usage_amount = usage[circuit.name]
                for child in circuit.child_circuits.values():
                    usage_amount -= usage[child.name]
                return usage_amount

            labeled_gauge.set_function(get_remainder)


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

    print("Logging in to Emporia...")
    emporia.do_logins_and_build_circuits()

    if args.list_devices:
        assert emporia.locations, "Failed to build locations"
        for location in emporia.locations.values():
            print("Location: " + location.name)
            print("Circuits:")
            recursive_print_circuits(location.circuits.values())
    else:
        exit_event = Event()
        signal.signal(signal.SIGINT, lambda _s, _f: exit_event.set())
        signal.signal(signal.SIGHUP, lambda _s, _f: exit_event.set())

        emporia.build_gauges()
        start_http_server(8000)
        print("Server is running.")
        exit_event.wait()
