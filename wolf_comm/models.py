from dataclasses import dataclass
from abc import ABC, abstractmethod
from typing import List

@dataclass
class Device:
    device_id: int
    gateway: int
    name: str

    def __str__(self) -> str:
        return f'Name: {self.name}, Id: {self.device_id}, Gateway {self.gateway}'

@dataclass
class Parameter(ABC):
    value_id: int
    name: str
    parent: str
    parameter_id: int
    bundle_id: int
    read_only: bool

    def __str__(self) -> str:
        return f"{self.__class__.__name__} -> {self.name}[{self.parameter_id}][{self.bundle_id}][{self.read_only}][{self.value_id}] of {self.parent}"

@dataclass
class SimpleParameter(Parameter):
    pass

@dataclass
class UnitParameter(Parameter, ABC):
    @property
    @abstractmethod
    def unit(self) -> str:
        ...

    def __str__(self) -> str:
        return super().__str__() + f" unit: [{self.unit}]"

# Unit-specific parameters with just the unit property different
@dataclass
class Temperature(UnitParameter):
    @property
    def unit(self) -> str:
        return "°C"

@dataclass
class Pressure(UnitParameter):
    @property
    def unit(self) -> str:
        return "bar"

@dataclass
class HoursParameter(UnitParameter):
    @property
    def unit(self) -> str:
        return "H"

@dataclass
class PercentageParameter(UnitParameter):
    @property
    def unit(self) -> str:
        return "%"

@dataclass
class PowerParameter(UnitParameter):
    @property
    def unit(self) -> str:
        return "kW"

@dataclass
class EnergyParameter(UnitParameter):
    @property
    def unit(self) -> str:
        return "kWh"

@dataclass
class RPMParameter(UnitParameter):
    @property
    def unit(self) -> str:
        return "U/min"

@dataclass
class FlowParameter(UnitParameter):
    @property
    def unit(self) -> str:
        return "l/min"

@dataclass
class FrequencyParameter(UnitParameter):
    @property
    def unit(self) -> str:
        return "Hz"

@dataclass
class ListItem:
    value: int
    name: str

    def __str__(self) -> str:
        return f'{self.value} -> {self.name}'

@dataclass
class ListItemParameter(Parameter):
    items: List[ListItem]

    def __str__(self) -> str:
        return super().__str__() + " items: " + ", ".join(str(item) for item in self.items)

@dataclass
class Value:
    value_id: int
    value: str
    state: str

    def __str__(self) -> str:
        return f'Value id: {self.value_id}, value: {self.value}, state {self.state}'

