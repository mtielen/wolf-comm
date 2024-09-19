import datetime
from typing import Union

import httpx
import logging
import re
import json
import aiohttp
from httpx import Headers

from wolf_comm.constants import BASE_URL_PORTAL, ID, GATEWAY_ID, NAME, SYSTEM_ID, MENU_ITEMS, TAB_VIEWS, BUNDLE_ID, \
    BUNDLE, VALUE_ID_LIST, GUI_ID_CHANGED, SESSION_ID, VALUE_ID, VALUE, STATE, VALUES, PARAMETER_ID, UNIT, \
    CELSIUS_TEMPERATURE, BAR, PERCENTAGE, LIST_ITEMS, DISPLAY_TEXT, PARAMETER_DESCRIPTORS, TAB_NAME, HOUR, \
    LAST_ACCESS, ERROR_CODE, ERROR_TYPE, ERROR_MESSAGE, ERROR_READ_PARAMETER, SYSTEM_LIST, GATEWAY_STATE, IS_ONLINE
from wolf_comm.create_session import create_session, update_session
from wolf_comm.helpers import bearer_header
from wolf_comm.models import Temperature, Parameter, SimpleParameter, Device, Pressure, ListItemParameter, \
    PercentageParameter, Value, ListItem, HoursParameter
from wolf_comm.token_auth import Tokens, TokenAuth

_LOGGER = logging.getLogger(__name__)


class WolfClient:
    session_id: int or None
    tokens: Tokens or None
    last_access: datetime or None
    last_failed: bool
    last_session_refesh: datetime or None
    language: dict or None

    @property
    def client(self):
        if hasattr(self, '_client') and self._client != None:
            return self._client
        elif hasattr(self, '_client_lambda') and self._client_lambda != None:
            return self._client_lambda()
        else:
            raise RuntimeError("No valid client configuration")


    def __init__(self, username: str, password: str, client = None, client_lambda = None, language="de"):
        _LOGGER.debug('Creating WolfClient')
        if client != None and client_lambda != None:
            raise RuntimeError("Only one of client and client_lambda is allowed!")
        elif client != None:
            self._client = client
        elif client_lambda != None:
            self._client_lambda = client_lambda
        else:
            self._client = httpx.AsyncClient()

        self.tokens = None
        self.token_auth = TokenAuth(username, password)
        self.session_id = None
        self.last_access = None
        self.last_failed = False
        self.last_session_refesh = None
        try:
            _LOGGER.info('Preloading language %s', language)
            self.load_localized_json(language)
        except Exception as e:
            _LOGGER.error('Failed to load language %s', language)
            _LOGGER.error(e)

    async def __request(self, method: str, path: str, **kwargs) -> Union[dict, list]:
        if self.tokens is None or self.tokens.is_expired():
            await self.__authorize_and_session()

        headers = kwargs.get('headers')

        if headers is None:
            headers = bearer_header(self.tokens.access_token)
        else:
            headers = {**bearer_header(self.tokens.access_token), **dict(headers)}

        if self.last_session_refesh is None or self.last_session_refesh <= datetime.datetime.now():
            await update_session(self.client, self.tokens.access_token, self.session_id)
            self.last_session_refesh = datetime.datetime.now() + datetime.timedelta(seconds=60)
            _LOGGER.debug('Sessionid: %s extented', self.session_id)

        resp = await self.__execute(headers, kwargs, method, path)
        if resp.status_code == 401 or resp.status_code == 500:
            _LOGGER.info('Retrying failed request (status code %d)',
                         resp.status_code)
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
        return await self.client.request(method, f"{BASE_URL_PORTAL}/{path}", **dict(kwargs, headers=Headers(headers)))

    async def __authorize_and_session(self):
        self.tokens = await self.token_auth.token(self.client)
        self.session_id = await create_session(self.client, self.tokens.access_token)
        self.last_session_refesh = datetime.datetime.now() + datetime.timedelta(seconds=60)

    # api/portal/GetSystemList
    async def fetch_system_list(self) -> list[Device]:
        system_list = await self.__request('get', 'api/portal/GetSystemList')
        _LOGGER.debug('Fetched systems: %s', system_list)
        return [Device(system[ID], system[GATEWAY_ID], system[NAME]) for system in system_list]

    # api/portal/GetSystemStateList
    async def fetch_system_state_list(self, system_id, gateway_id) -> bool:
        payload = {SESSION_ID: self.session_id, SYSTEM_LIST: [{SYSTEM_ID: system_id, GATEWAY_ID: gateway_id}]}
        system_state_response = await self.__request('post', 'api/portal/GetSystemStateList', json=payload)
        _LOGGER.debug('Fetched system state: %s', system_state_response)
        return system_state_response[0][GATEWAY_STATE][IS_ONLINE]


    # api/portal/GetGuiDescriptionForGateway?GatewayId={gateway_id}&SystemId={system_id}
    async def fetch_parameters(self, gateway_id, system_id) -> list[Parameter]:
        payload = {GATEWAY_ID: gateway_id, SYSTEM_ID: system_id}
        desc = await self.__request('get', 'api/portal/GetGuiDescriptionForGateway', params=payload)
        _LOGGER.debug('Fetched parameters: %s', desc)
        tab_views = desc[MENU_ITEMS][0][TAB_VIEWS]
        result = [WolfClient._map_view(view) for view in tab_views]

        result.reverse()
        distinct_ids = []
        flattened = []
        for sublist in result:
            distinct_names = []
            for val in sublist:
                #get from language if exists into a var
                name = val.name
                if self.language is not None and val.name in self.language:
                    name = self.language[val.name]

                if val.value_id not in distinct_ids and name not in distinct_names:
                    distinct_ids.append(val.value_id)
                    distinct_names.append(name)
                    flattened.append(val)
        return flattened

    # api/portal/CloseSystem
    async def close_system(self):
        data = {
            SESSION_ID: self.session_id
        }
        res = await self.__request('post', 'api/portal/CloseSystem', json=data)
        _LOGGER.debug('Close system response: %s', res)

    @staticmethod
    def extract_messages_json(text):
        json_match = re.search(r'messages:\s*({.*?})\s*}', text, re.DOTALL)

        if json_match:
            json_string = json_match.group(1)
            json_string = re.sub(r'([a-zA-Z0-9_.%-]+)\s*:', r'"\1":', json_string)
            return json.loads(json_string)

        return None

    @staticmethod
    async def fetch_localized_text(language: str):
        url = f'https://www.wolf-smartset.com/js/localized-text/text.culture.{language}.js'

        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                _LOGGER.info('Response status %s', response.status)
                if response.status == 200 or response.status == 304:
                    return await response.text()
                else:
                    return ""

    async def load_localized_json(self, language_input: str):
        _LOGGER.info('Inside load_localized_json %s', language_input)
        res = await self.fetch_localized_text(language_input)
        _LOGGER.info('Fetched localized text %s', res)
        parsed_json = WolfClient.extract_messages_json(res)

        _LOGGER.info('Parsed json %s', language_input)

        if parsed_json is not None:
            self.language = parsed_json
            _LOGGER.info('Loaded language %s', language_input)
            _LOGGER.info('Loaded language json %s', parsed_json)

    # api/portal/GetParameterValues
    async def fetch_value(self, gateway_id, system_id, parameters: list[Parameter]):
        data = {
            BUNDLE_ID: 1000,
            BUNDLE: False,
            VALUE_ID_LIST: [param.value_id for param in parameters],
            GATEWAY_ID: gateway_id,
            SYSTEM_ID: system_id,
            GUI_ID_CHANGED: False,
            SESSION_ID: self.session_id,
            LAST_ACCESS: self.last_access
        }
        res = await self.__request('post', 'api/portal/GetParameterValues', json=data,
                                   headers={"Content-Type": "application/json"})

        _LOGGER.debug('Fetched values: %s', res)

        if ERROR_CODE in res or ERROR_TYPE in res:
            if ERROR_MESSAGE in res and res[ERROR_MESSAGE] == ERROR_READ_PARAMETER:
                raise ParameterReadError(res)
            raise FetchFailed(res)

        self.last_access = res[LAST_ACCESS]
        return [Value(v[VALUE_ID], v[VALUE], v[STATE]) for v in res[VALUES] if VALUE in v]

    @staticmethod
    def _map_parameter(parameter: dict, parent: str) -> Parameter:
        value_id = parameter[VALUE_ID]
        name = parameter[NAME]
        parameter_id = parameter[PARAMETER_ID]
        if UNIT in parameter:
            unit = parameter[UNIT]
            if unit == CELSIUS_TEMPERATURE:
                return Temperature(value_id, name, parent, parameter_id)
            elif unit == BAR:
                return Pressure(value_id, name, parent, parameter_id)
            elif unit == PERCENTAGE:
                return PercentageParameter(value_id, name, parent, parameter_id)
            elif unit == HOUR:
                return HoursParameter(value_id, name, parent, parameter_id)
        elif LIST_ITEMS in parameter:
            items = [ListItem(list_item[VALUE], list_item[DISPLAY_TEXT]) for list_item in parameter[LIST_ITEMS]]
            return ListItemParameter(value_id, name, parent, items, parameter_id)
        return SimpleParameter(value_id, name, parent, parameter_id)

    @staticmethod
    def _map_view(view: dict):
        if 'SVGHeatingSchemaConfigDevices' in view:
            units = dict([(unit['valueId'], unit['unit']) for unit
                          in view['SVGHeatingSchemaConfigDevices'][0]['parameters'] if 'unit' in unit])

            new_params = []
            for param in view[PARAMETER_DESCRIPTORS]:
                if param[VALUE_ID] in units:
                    param[UNIT] = units[param[VALUE_ID]]
                new_params.append(WolfClient._map_parameter(param, view[TAB_NAME]))
            return new_params
        else:
            return [WolfClient._map_parameter(p, view[TAB_NAME]) for p in view[PARAMETER_DESCRIPTORS]]


class FetchFailed(Exception):
    """Server returned 500 code with message while executing query"""
    pass

class ParameterReadError(Exception):
    """Server returned RedParameterValues error"""
    pass
