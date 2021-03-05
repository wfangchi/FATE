#
#  Copyright 2019 The FATE Authors. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

import builtins
import copy
import importlib
import importlib.util
import json
import os
import typing

from fate_flow.utils.dsl_exception import (
    ModuleConfigError,
    ModuleNotExistError,
    ParamClassNotExistError,
    ParameterCheckError,
    RedundantParameterError,
    RoleParameterNotConsistencyError,
    RoleParameterNotListError,
    SubmitConfNotExistError,
)
from ruamel import yaml


class ParameterUtil(object):
    @classmethod
    def get_args_input(cls, submit_dict, module="args"):
        if "role_parameters" not in submit_dict:
            return {}

        roles = submit_dict["role_parameters"].keys()
        if not roles:
            return {}

        args_input = {}
        args_datakey = set()

        for role in roles:
            if not submit_dict["role_parameters"][role].get(module):
                continue
            partyid_list = submit_dict["role"][role]

            args_parameters = submit_dict["role_parameters"][role].get(module)
            args_input[role] = []

            if "data" in args_parameters:
                dataset = args_parameters.get("data")
                for data_key in dataset:
                    datalist = dataset[data_key]

                    if len(datalist) != len(partyid_list):
                        raise RoleParameterNotConsistencyError(
                            role=role, parameter=data_key
                        )

                    args_datakey.add(data_key)

                    for i in range(len(datalist)):
                        value = datalist[i]
                        if len(args_input[role]) <= i:
                            args_input[role].append({module: {"data": {}}})

                        args_input[role][i][module]["data"][data_key] = value

        return args_input, args_datakey

    @staticmethod
    def get_job_parameters(submit_dict):
        ret = {}
        job_parameters = submit_dict.get("job_parameters", {})
        for role in submit_dict["role"]:
            partyid_list = submit_dict["role"][role]
            ret[role] = {
                party_id: copy.deepcopy(job_parameters) for party_id in partyid_list
            }

        return ret


class ParameterUtilV2(object):
    @classmethod
    def get_input_parameters(cls, submit_dict, components=None):
        if (
            submit_dict.get("component_parameters", {}).get("role") is None
            or components is None
        ):
            return {}

        roles = submit_dict["component_parameters"]["role"].keys()
        if not roles:
            return {}

        input_parameters: typing.MutableMapping[str, typing.Any] = {"dsl_version": 2}

        cpn_dict = {}
        for reader_cpn in components:
            cpn_dict[reader_cpn] = {}
        for role in roles:
            role_parameters = submit_dict["component_parameters"]["role"][role]
            input_parameters[role] = [
                copy.deepcopy(cpn_dict) for i in range(len(submit_dict["role"][role]))
            ]

            for idx in role_parameters.keys():
                parameters = role_parameters[idx]
                for reader in components:
                    if reader not in parameters:
                        continue

                    if idx == "all":
                        partyid_list = submit_dict["role"][role]
                        for i in range(len(partyid_list)):
                            input_parameters[role][i][reader] = parameters[reader]
                    elif len(idx.split("|")) == 1:
                        input_parameters[role][int(idx)][reader] = parameters[reader]
                    else:
                        id_set = list(map(int, idx.split("|")))
                        for _id in id_set:
                            input_parameters[role][_id][reader] = parameters[reader]

        return input_parameters

    @staticmethod
    def get_job_parameters(submit_dict):
        ret = {}
        job_parameters = submit_dict.get("job_parameters", {})
        common_job_parameters = job_parameters.get("common", {})
        role_job_parameters = job_parameters.get("role", {})
        for role in submit_dict["role"]:
            partyid_list = submit_dict["role"][role]
            if not role_job_parameters:
                ret[role] = {
                    party_id: copy.deepcopy(common_job_parameters)
                    for party_id in partyid_list
                }
                continue

            ret[role] = {}
            for idx in range(len(partyid_list)):
                role_idxs = role_job_parameters.get(role, {}).keys()
                parameters = copy.deepcopy(common_job_parameters)
                for role_id in role_idxs:
                    if role_id == "all" or str(idx) in role_id.split("|"):
                        parameters = merge_dict_recursive(
                            parameters, role_job_parameters.get(role, {})[role_id]
                        )

                ret[role][partyid_list[idx]] = parameters

        return ret


def override_parameter(
    setting_conf_prefix,
    submit_dict,
    module,
    module_alias,
    version,
    redundant_param_check,
) -> typing.MutableMapping[str, typing.List[typing.MutableMapping[str, typing.Any]]]:
    """
    return runtime role parameters
    """

    _module_setting = _get_setting_conf(setting_conf_prefix, module, module_alias)

    param_class_path = _module_setting["param_class"]
    param_class, param_obj = _get_param_object(param_class_path, module, module_alias)

    default_runtime_dict = _convert_object_as_dict(param_obj)

    if not submit_dict:
        raise SubmitConfNotExistError()

    runtime_role_parameters = {}

    role_on_module = copy.deepcopy(submit_dict["role"])
    for role in submit_dict["role"]:
        _code_path = get_code_path(
            module_setting=_module_setting,
            role=role,
            module=module,
            module_alias=module_alias,
        )
        if not _code_path:
            del role_on_module[role]
            continue
        partyid_list = submit_dict["role"][role]
        runtime_role_parameters[role] = []

        for idx in range(len(partyid_list)):
            runtime_dict: typing.MutableMapping[str, typing.Any] = {
                param_class: copy.deepcopy(default_runtime_dict)
            }
            for key, value in submit_dict.items():
                if key not in ["algorithm_parameters", "role_parameters"]:
                    runtime_dict[key] = value

            role_param_obj = copy.deepcopy(param_obj)

            common_parameters = (
                submit_dict.get("component_parameters", {}).get("common", {})
                if version == 2
                else submit_dict.get("algorithm_parameters", {})
            )
            if module_alias in common_parameters:
                parameters = common_parameters.get(module_alias)
                merge_dict = _merge_parameters(
                    runtime_dict[param_class],
                    parameters,
                    role_param_obj,
                    component=module_alias,
                    module=module,
                    version=version,
                    redundant_param_check=redundant_param_check,
                )
                runtime_dict[param_class] = merge_dict

            if version == 2:
                component_parameters = submit_dict.get("component_parameters", {}).get(
                    "role", {}
                )
                role_parameters = component_parameters.get(role, {})
                role_idxs = role_parameters.keys()
                for role_id in role_idxs:
                    if role_id == "all" or str(idx) in role_id.split("|"):
                        role_dict = role_parameters[role_id]

                        if module_alias in role_dict:
                            parameters = role_dict.get(module_alias)
                            merge_dict = _merge_parameters(
                                runtime_dict[param_class],
                                parameters,
                                role_param_obj,
                                role_id,
                                role,
                                role_num=len(partyid_list),
                                component=module_alias,
                                module=module,
                                version=version,
                                redundant_param_check=redundant_param_check,
                            )

                            runtime_dict[param_class] = merge_dict

            else:
                role_dict = submit_dict.get("role_parameters", {}).get(role, {})
                if module_alias in role_dict:
                    role_parameters = role_dict.get(module_alias)
                    merge_dict = _merge_parameters(
                        runtime_dict[param_class],
                        role_parameters,
                        role_param_obj,
                        idx,
                        role,
                        role_num=len(partyid_list),
                        component=module_alias,
                        module=module,
                        version=version,
                        redundant_param_check=redundant_param_check,
                    )
                    runtime_dict[param_class] = merge_dict

            try:
                role_param_obj.check()
            except Exception as e:
                raise ParameterCheckError(
                    component=module_alias, module=module, other_info=e
                )

            runtime_dict["local"] = submit_dict.get("local", {})
            my_local = {"role": role, "party_id": partyid_list[idx]}
            runtime_dict["local"].update(my_local)
            runtime_dict["CodePath"] = _code_path
            runtime_dict["module"] = module

            runtime_role_parameters[role].append(runtime_dict)

    for role, role_params in runtime_role_parameters.items():
        for param_dict in role_params:
            param_dict["role"] = role_on_module

    return runtime_role_parameters


def get_param_class_name(setting_conf_prefix, module):
    _module_setting = _get_setting_conf(
        setting_conf_prefix=setting_conf_prefix, module=module, module_alias=module
    )
    param_class_path = _module_setting["param_class"]
    param_class = param_class_path.split("/", -1)[-1]

    return param_class


def get_code_path(
    role=None,
    setting_conf_prefix=None,
    module=None,
    module_alias=None,
    module_setting=None,
):
    if not module_setting:
        _module_setting = _get_setting_conf(setting_conf_prefix, module, module_alias)
    else:
        _module_setting = module_setting

    _support_roles = _module_setting["role"].keys()
    _role_setting = None
    for _rolelist in _support_roles:
        if role not in _rolelist.split("|"):
            continue
        else:
            _role_setting = _module_setting["role"].get(_rolelist)

    if not _role_setting:
        return None

    _code_path = os.path.join(
        _module_setting.get("module_path"), _role_setting.get("program")
    )

    return _code_path


def merge_dict_recursive(dict1: dict, dict2: dict):
    """
    merge dict recursive

    Notes: dict2 first
    """
    merged = copy.deepcopy(dict2)
    for key, value in dict1.items():
        if key not in dict2:
            # new key for dict2, add
            merged[key] = copy.deepcopy(value)
        elif isinstance(value, dict):
            # common key with dict value, merge
            merged[key] = merge_dict_recursive(value, dict2[key])
        # common key with non-dict value, preserve value in dict2
    return merged


def _convert_object_as_dict(obj):
    """
    convert object as dict in recursive
    """
    ret_dict = {}

    variable_dict = obj.__dict__
    for variable in variable_dict:
        attr = getattr(obj, variable)
        if attr and type(attr).__name__ not in dir(builtins):
            ret_dict[variable] = _convert_object_as_dict(attr)
        else:
            ret_dict[variable] = attr

    return ret_dict


def _get_param_object(param_class_path, module, module_alias):
    param_class = param_class_path.split("/", -1)[-1]

    param_module_path = ".".join(param_class_path.split("/", -1)[:-1]).replace(
        ".py", ""
    )
    if not importlib.util.find_spec(param_module_path):
        raise ParamClassNotExistError(
            component=module_alias,
            module=module,
            other_info="{} does not exist".format(param_module_path),
        )

    param_module = importlib.import_module(param_module_path)

    if getattr(param_module, param_class) is None:
        raise ParamClassNotExistError(
            component=module_alias,
            module=module,
            other_info="{} does not exist is {}".format(param_class, param_module),
        )

    param_obj = getattr(param_module, param_class)()

    return param_class, param_obj


def _get_setting_conf(setting_conf_prefix, module, module_alias):
    """
    get setting conf of federatedml module
    """
    # TODO: support extension
    # roadmap
    # 1. treat `setting_conf_prefix` as ml_conf
    # 2. rename `setting_conf_prefix`
    # 3. use `enterpoint` and `pkg_resourece` to support extension
    #

    # extensions field in ml_conf:
    # - name: buildin
    #   setting_conf_base: "federatedml/conf/setting_conf"
    #   auth_conf_base: "python/federatedml/transfer_variable/auth_conf"
    # typing alias:
    _ExtensionType = typing.List[typing.MutableMapping[str, str]]

    with open(setting_conf_prefix) as f:
        extensions: _ExtensionType = yaml.safe_load(f).get("extensions", [])

    # base directory for those relative paths in ml_conf
    _ml_conf_dir = os.path.abspath(
        os.path.join(setting_conf_prefix, os.path.pardir, os.path.pardir)
    )

    def _as_absolute(_p):
        _abs_path = _p
        if not os.path.exists(_abs_path):
            _abs_path = os.path.join(_ml_conf_dir, _p)
        if not os.path.exists(_abs_path):
            raise FileNotFoundError(f"path {_p} not exists, please check ml_conf.yaml")
        return _abs_path

    setting_conf_bases = [
        _as_absolute(extension["setting_conf_base"]) for extension in extensions
    ]

    # search modules in all setting conf bases
    _module_setting_path = None
    for base_dir in setting_conf_bases:
        _module_setting_path = os.path.join(base_dir, module + ".json")
        if os.path.exists(_module_setting_path):
            break
    if _module_setting_path is None or not os.path.isfile(_module_setting_path):
        raise ModuleNotExistError(component=module_alias, module=module)

    # load setting conf
    try:
        with open(_module_setting_path, "r") as fin:
            _module_setting = json.loads(fin.read())
    except Exception as e:
        raise ModuleConfigError(
            component=module_alias, module=module, other_info=e
        ) from e

    return _module_setting


def _merge_parameters(
    runtime_dict,
    role_parameters,
    param_obj,
    idx=-1,
    role=None,
    role_num=0,
    component=None,
    module=None,
    version=1,
    redundant_param_check=True,
):
    param_variables = param_obj.__dict__
    for key, val_list in role_parameters.items():
        if not redundant_param_check:
            if key not in param_variables:
                continue

        if key not in param_variables:
            raise RedundantParameterError(
                component=component, module=module, other_info=key
            )

        attr = getattr(param_obj, key)
        if type(attr).__name__ in dir(builtins) or not attr:
            if version == 1 and idx != -1:
                if not isinstance(val_list, list):
                    raise RoleParameterNotListError(role=role, parameter=key)

                if len(val_list) != role_num:
                    raise RoleParameterNotConsistencyError(role=role, parameter=key)

                runtime_dict[key] = val_list[idx]
                setattr(param_obj, key, val_list[idx])
            else:
                runtime_dict[key] = val_list
                setattr(param_obj, key, val_list)
        else:
            if key not in runtime_dict:
                runtime_dict[key] = {}

            runtime_dict[key] = _merge_parameters(
                runtime_dict.get(key),
                val_list,
                attr,
                idx,
                role=role,
                role_num=role_num,
                component=component,
                module=module,
                version=version,
                redundant_param_check=redundant_param_check,
            )
            setattr(param_obj, key, attr)

    return runtime_dict
