import datetime
import json
import logging
import re
from typing import Union, Optional

import aiohttp
import httpx
from httpx import Headers

from wolf_comm.constants import BASE_URL_PORTAL, ID, GATEWAY_ID, NAME, SYSTEM_ID, MENU_ITEMS, TAB_VIEWS, BUNDLE_ID, \
    BUNDLE, VALUE_ID_LIST, GUI_ID_CHANGED, SESSION_ID, VALUE_ID, GROUP, VALUE, STATE, VALUES, PARAMETER_ID, UNIT, \
    CELSIUS_TEMPERATURE, BAR, RPM, FLOW, FREQUENCY, PERCENTAGE, LIST_ITEMS, DISPLAY_TEXT, PARAMETER_DESCRIPTORS, TAB_NAME, HOUR, KILOWATT, KILOWATTHOURS, \
    LAST_ACCESS, ERROR_CODE, ERROR_TYPE, ERROR_MESSAGE, ERROR_READ_PARAMETER, SYSTEM_LIST, GATEWAY_STATE, IS_ONLINE, WRITE_PARAMETER_VALUES, ISREADONLY
from wolf_comm.create_session import create_session, update_session
from wolf_comm.helpers import bearer_header
from wolf_comm.models import Temperature, Parameter, SimpleParameter, Device, Pressure, ListItemParameter, \
    PercentageParameter, Value, ListItem, HoursParameter, PowerParameter, EnergyParameter, RPMParameter, FlowParameter, FrequencyParameter
from wolf_comm.token_auth import Tokens, TokenAuth

_LOGGER = logging.getLogger(__name__)
SPLIT = "---"


class WolfClient:
    session_id: Optional[int]
    tokens: Optional[Tokens]
    last_access: Optional[datetime.datetime]
    last_failed: bool
    last_session_refesh: Optional[datetime.datetime]
    regional: Optional[dict]
    region_set: str
    expert_mode: bool

    @property
    def client(self):
        if hasattr(self, "_client") and self._client is not None:
            return self._client
        elif hasattr(self, "_client_lambda") and self._client_lambda is not None:
            return self._client_lambda()
        else:
            raise RuntimeError("No valid client configuration")

    def __init__(self, username: str, password: str, expert_p=None, region=None, client=None, client_lambda=None):
        if client is not None and client_lambda is not None:
            raise RuntimeError("Only one of client and client_lambda is allowed!")
        elif client is not None:
            self._client = client
        elif client_lambda is not None:
            self._client_lambda = client_lambda
        else:
            self._client = httpx.AsyncClient()

        self.tokens = None
        self.token_auth = TokenAuth(username, password)
        self.session_id = None
        self.last_access = None
        self.last_failed = False
        self.last_session_refesh = None
        self.regional = None
        self.expert_mode = expert_p if expert_p is not None else False
        self.region_set = region if region is not None else "en"

    async def __request(self, method: str, path: str, **kwargs) -> Union[dict, list]:
        if self.tokens is None or self.tokens.is_expired():
            await self.__authorize_and_session()

        headers = kwargs.get("headers", {})
        headers = {**bearer_header(self.tokens.access_token), **headers}

        if (
            self.last_session_refesh is None
            or self.last_session_refesh <= datetime.datetime.now()
        ):
            await update_session(self.client, self.tokens.access_token, self.session_id)
            self.last_session_refesh = datetime.datetime.now() + datetime.timedelta(
                seconds=60
            )
            _LOGGER.debug("Session ID: %s extended", self.session_id)

        if "json" in kwargs and self.session_id is not None:
            if isinstance(kwargs["json"], dict):
                kwargs["json"][SESSION_ID] = self.session_id

        resp = await self.__execute(headers, kwargs, method, path)
        if resp.status_code in {401, 500}:
            _LOGGER.info("Retrying failed request (status code %d)", resp.status_code)
            await self.__authorize_and_session()
            headers = {**bearer_header(self.tokens.access_token), **dict(headers)}
            try:
                execution = await self.__execute(headers, kwargs, method, path)
                return execution.json()
            except FetchFailed as e:
                self.last_failed = True
                raise e
        else:
            self.last_failed = False
            return resp.json()

    async def __execute(self, headers, kwargs, method, path):
        return await self.client.request(
            method,
            f"{BASE_URL_PORTAL}/{path}",
            **dict(kwargs, headers=Headers(headers)),
        )

    async def __authorize_and_session(self):
        self.tokens = await self.token_auth.token(self.client)
        self.session_id = await create_session(self.client, self.tokens.access_token)
        self.last_session_refesh = datetime.datetime.now() + datetime.timedelta(
            seconds=60
        )

    # api/portal/GetSystemList
    async def fetch_system_list(self) -> list[Device]:
        system_list = await self.__request("get", "api/portal/GetSystemList")
        _LOGGER.debug("Fetched systems: %s", system_list)
        return [
            Device(system[ID], system[GATEWAY_ID], system[NAME])
            for system in system_list
        ]

    # api/portal/GetSystemStateList
    async def fetch_system_state_list(self, system_id, gateway_id) -> bool:
        payload = {
            SESSION_ID: self.session_id,
            SYSTEM_LIST: [{SYSTEM_ID: system_id, GATEWAY_ID: gateway_id}],
        }
        system_state_response = await self.__request(
            "post", "api/portal/GetSystemStateList", json=payload
        )
        _LOGGER.debug("Fetched system state: %s", system_state_response)
        return system_state_response[0][GATEWAY_STATE][IS_ONLINE]

    # api/portal/GetGuiDescriptionForGateway?GatewayId={gateway_id}&SystemId={system_id}
    async def fetch_parameters(self, gateway_id, system_id) -> list[Parameter]:
        await self.load_localized_json(self.region_set)
        payload = {GATEWAY_ID: gateway_id, SYSTEM_ID: system_id}
        desc = await self.__request(
            "get", "api/portal/GetGuiDescriptionForGateway", params=payload
        )
        _LOGGER.debug("Fetched parameters: %s", desc)
        if self.expert_mode:
            descriptors = WolfClient._extract_parameter_descriptors(desc)
            _LOGGER.debug("Found parameter descriptors: %s", len(descriptors))
            descriptors.sort(key=lambda x: x['ValueId'])
            result = [[WolfClient._map_parameter(p, None) for p in descriptors]]
        else:
            tab_views = desc[MENU_ITEMS][0][TAB_VIEWS]
            result = [WolfClient._map_view(view) for view in tab_views]
        result.reverse()
        distinct_ids = []
        flattened = []
        for sublist in result:
            for val in sublist:
                if val is None:
                    _LOGGER.debug("Encountered None value in parameters")
                    continue
                spaceSplit = val.name.split(SPLIT, 2)
                if len(spaceSplit) == 2:
                    key = (
                        spaceSplit[0].split("_")[1]
                        if spaceSplit[0].count("_") > 0
                        else spaceSplit[0]
                    )
                    name = (
                        self.replace_with_localized_text(key)
                        + " "
                        + self.replace_with_localized_text(spaceSplit[1])
                    )
                    val.name = name
                else:
                    val.name = self.replace_with_localized_text(val.name)

                if val.value_id not in distinct_ids:
                    distinct_ids.append(val.value_id)
                    flattened.append(val)
                else:
                    _LOGGER.debug(
                        "Skipping parameter with id %s and name %s",
                        val.value_id,
                        val.name,
                    )
        flattened_fixed = self.fix_duplicated_parameters(flattened)
        return flattened_fixed

    def fix_duplicated_parameters(self, parameters):
        """Fix duplicated parameters."""
        seen = set()
        new_parameters = []
        for parameter in parameters:
            if parameter is None:
                _LOGGER.debug("Encountered None value in parameters")
                continue                
            if parameter.value_id not in seen:
                new_parameters.append(parameter)
                seen.add(parameter.value_id)
                _LOGGER.debug("Adding parameter: %s", parameter.value_id)
            else:
                _LOGGER.debug(
                    "Duplicated parameter found: %s. Skipping this parameter",
                    parameter.value_id,
                )
        return new_parameters

    def replace_with_localized_text(self, text: str):
        if self.regional is not None and text in self.regional:
            return self.regional[text]
        return text

    # api/portal/CloseSystem
    async def close_system(self):
        data = {SESSION_ID: self.session_id}
        res = await self.__request("post", "api/portal/CloseSystem", json=data)
        _LOGGER.debug("Close system response: %s", res)

    @staticmethod
    def extract_messages_json(text):
        json_match = re.search(r"messages:\s*({.*?})\s*}", text, re.DOTALL)

        if json_match:
            json_string = json_match.group(1)
            return WolfClient.try_and_parse(json_string, 1000)
        else:
            return None

    @staticmethod
    def try_and_parse(text, times):
        if times == 0:
            return text
        try:
            return json.loads(text)
        except json.JSONDecodeError as e:
            line = e.lineno - 1

            text_lines = text.split("\n")

            if line < len(text_lines):
                text_lines.pop(line)

            new_text = "\n".join(text_lines)
            return WolfClient.try_and_parse(new_text, times - 1)

    @staticmethod
    async def fetch_localized_text(culture: str):
        async with aiohttp.ClientSession() as session:
            url = f"https://www.wolf-smartset.com/js/localized-text/text.culture.{culture}.js"
            async with session.get(url) as response:
                if response.status == 200 or response.status == 304:
                    return await response.text()
                
            if culture != 'en':
                _LOGGER.debug("Culture %s not found, falling back to English", culture)
                url = "https://www.wolf-smartset.com/js/localized-text/text.culture.en.js"
                async with session.get(url) as response:
                    if response.status == 200 or response.status == 304:
                        return await response.text()
            
            return ""

    async def load_localized_json(self, region_input: str):
        res = await self.fetch_localized_text(region_input)

        parsed_json = WolfClient.extract_messages_json(res)

        if parsed_json is not None:
            self.regional = parsed_json
        else:
            _LOGGER.error("No support for region: %s", region_input)

    # api/portal/GetParameterValues
    async def fetch_value(self, gateway_id, system_id, parameters: list[Parameter]):
        values_combined = []
        bundles = {}
        last_access_map = {}
        
        for param in parameters:
            bundles.setdefault(param.bundle_id, []).append(param)
            last_access_map.setdefault(param.bundle_id, None)

        for bundle_id, params in bundles.items():
            if not params:
                continue
                
            data = {
                BUNDLE_ID: bundle_id,
                BUNDLE: False,
                VALUE_ID_LIST: [param.value_id for param in params],  
                GATEWAY_ID: gateway_id,
                SYSTEM_ID: system_id,
                GUI_ID_CHANGED: False,
                SESSION_ID: self.session_id,
                LAST_ACCESS: last_access_map[bundle_id],
            }
            
            _LOGGER.debug('Requesting %s values for BUNDLE_ID: %s', len(params), bundle_id)
            res = await self.__request("post", "api/portal/GetParameterValues", json=data, headers={"Content-Type": "application/json"})

            if ERROR_CODE in res or ERROR_TYPE in res:
                error_msg = f"Error {res.get(ERROR_CODE, '')}: {res.get(ERROR_MESSAGE, str(res))}"
                if ERROR_MESSAGE in res and res[ERROR_MESSAGE] == ERROR_READ_PARAMETER:
                    raise ParameterReadError(error_msg)
                raise FetchFailed(error_msg)

            values_combined.extend(
                Value(v[VALUE_ID], v[VALUE], v[STATE])
                for v in res[VALUES]
                if VALUE in v
            )
            last_access_map[bundle_id] = res[LAST_ACCESS]

        _LOGGER.debug('requested values for %s parameters, got values for %s ', len(parameters), len(values_combined))
        return values_combined

    # api/portal/WriteParameterValues
    async def write_value(self, gateway_id, system_id, bundle_id, Value):
        data = {
            WRITE_PARAMETER_VALUES: [
                {"ValueId": Value[VALUE_ID], "Value": Value[STATE]}
            ],
            SYSTEM_ID: system_id,
            GATEWAY_ID: gateway_id,
            BUNDLE_ID: bundle_id,
        }

        res = await self.__request(
            "post",
            "api/portal/WriteParameterValues",
            json=data,
            headers={"Content-Type": "application/json"},
        )

        _LOGGER.debug("Written values: %s", res)

        if ERROR_CODE in res or ERROR_TYPE in res:
            error_msg = f"Error {res.get(ERROR_CODE, '')}: {res.get(ERROR_MESSAGE, str(res))}"
            if ERROR_MESSAGE in res and res[ERROR_MESSAGE] == ERROR_READ_PARAMETER:
                raise ParameterWriteError(error_msg)
            raise WriteFailed(error_msg)

        return res

    @staticmethod
    def _map_parameter(parameter: dict, parent: str) -> Parameter:
        value_id = parameter[VALUE_ID]
        name = parameter[NAME]
        parameter_id = parameter[PARAMETER_ID]

        bundle_id = parameter.get(BUNDLE_ID, "1000")	
        readonly = parameter.get(ISREADONLY, True)

        if UNIT in parameter:
            unit = parameter[UNIT]
            if unit == CELSIUS_TEMPERATURE:
                return Temperature(value_id, name, parent, parameter_id, bundle_id, readonly)
            elif unit == BAR:
                return Pressure(value_id, name, parent, parameter_id, bundle_id, readonly)
            elif unit == PERCENTAGE:
                return PercentageParameter(value_id, name, parent, parameter_id, bundle_id, readonly)
            elif unit == HOUR:
                return HoursParameter(value_id, name, parent, parameter_id, bundle_id, readonly)
            elif unit == KILOWATT:
                return PowerParameter(value_id, name, parent, parameter_id, bundle_id, readonly)
            elif unit == KILOWATTHOURS:
                return EnergyParameter(value_id, name, parent, parameter_id, bundle_id, readonly)
            elif unit == RPM:
                return RPMParameter(value_id, name, parent, parameter_id, bundle_id, readonly)
            elif unit == FLOW:
                return FlowParameter(value_id, name, parent, parameter_id, bundle_id, readonly)
            elif unit == FREQUENCY:
                return FrequencyParameter(value_id, name, parent, parameter_id, bundle_id, readonly)
        elif LIST_ITEMS in parameter:
            items = [ListItem(list_item[VALUE], list_item[DISPLAY_TEXT]) for list_item in parameter[LIST_ITEMS]]
            return ListItemParameter(value_id, name, parent, items, parameter_id, bundle_id, readonly)
        else:
            return SimpleParameter(value_id, name, parent, parameter_id, bundle_id, readonly)

    @staticmethod
    def _map_view(view: dict):
        if "SVGHeatingSchemaConfigDevices" in view:
            units = dict(
                [
                    (unit["valueId"], unit["unit"])
                    for unit in view["SVGHeatingSchemaConfigDevices"][0]["parameters"]
                    if "unit" in unit
                ]
            )

            new_params = []
            for param in view[PARAMETER_DESCRIPTORS]:
                if param[VALUE_ID] in units:
                    param[UNIT] = units[param[VALUE_ID]]
                new_params.append(WolfClient._map_parameter(param, view[TAB_NAME]))
            return new_params
        else:
            return [
                WolfClient._map_parameter(p, view[TAB_NAME])
                for p in view[PARAMETER_DESCRIPTORS]
            ]

    @staticmethod
    def _extract_parameter_descriptors(desc):
        # recursively traverses datastructure returned by GetGuiDescriptionForGateway API and extracts all ParameterDescriptors arrays
        def traverse(item, path=''):
            # Object is a dict, crawl keys
            if type(item) is dict:
                bundleId = None
                if "BundleId" in item: 
                    bundleId = item["BundleId"]
		
                for key in item:
                    if key == "ParameterDescriptors":
                        _LOGGER.debug("Found ParameterDescriptors at path: %s", path)
                        # Store BundleId from parent in each item for getting values
                        for descriptor in item[key]:
                            descriptor["BundleId"] = bundleId
                        yield from item[key]
                    yield from traverse(item[key], path + key + '>')

            # Object is a list, crawl list items
            elif type(item) is list:
                i = 0
                for a in item:
                    _LOGGER.debug("Found listitem at path: %s", path)
                    yield from traverse(a, path + str(i) + '>')
                    i += 1

        return list(traverse(desc))


class WolfError(Exception):
    """Base exception class for Wolf client errors"""
    def __init__(self, message: str, response: dict = None):
        super().__init__(message)
        self.response = response

class FetchFailed(WolfError):
    """Exception raised when server returns an error while fetching data"""
    def __init__(self, message: str, response: dict = None):
        super().__init__(f"Failed to fetch data: {message}", response)

class ParameterError(WolfError):
    """Base class for parameter-related errors"""
    pass

class ParameterReadError(ParameterError):
    """Exception raised when reading parameters fails"""
    def __init__(self, message: str, response: dict = None):
        super().__init__(f"Failed to read parameters: {message}", response)

class ParameterWriteError(ParameterError):
    """Exception raised when writing parameters fails"""
    def __init__(self, message: str, response: dict = None):
        super().__init__(f"Failed to write parameters: {message}", response)

class WriteFailed(WolfError):
    """Exception raised when server returns an error while writing data"""
    def __init__(self, message: str, response: dict = None):
        super().__init__(f"Failed to write data: {message}", response)
