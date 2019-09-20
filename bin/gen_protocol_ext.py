#!/usr/bin/env python3

import argparse
import re
from os.path import basename


def parse():
    parser = argparse.ArgumentParser(
        description="The gernerator for seriesdb protocol api."
    )
    parser.add_argument("--proto_file", required=True,
                        type=argparse.FileType("r"))
    parser.add_argument("--enum_type_name", required=True)
    args = parser.parse_args()
    return args.proto_file, args.enum_type_name


def extract(content, enum_type_name):
    enum_type_def_pattern = r"enum\s+" + enum_type_name + "\s+{([^}]+)}"
    enum_type_def_match = re.search(enum_type_def_pattern, content)

    if enum_type_def_match:
        enum_pairs_pattern = r"([A-Z_0-9]+)\s*=\s*([0-9]+);"
        enum_pairs = re.findall(
            enum_pairs_pattern, enum_type_def_match.group(1))
        return enum_pairs
    else:
        return []


def capitalize(enum_name):
    return "".join(map(lambda s: s.capitalize(), enum_name.lower().split("_")))


def output(package_name, module_name, enum_pairs):
    import_decls_output = f"""import {package_name}.{module_name}_pb2 as {module_name}_pb2\n"""

    function_defs = []
    case_decls0 = []
    case_decls1 = []
    case = "if"
    is_first = True
    for (enum_name, enum_value) in enum_pairs:
        if enum_name[0:7] == "UNKNOWN":
            continue

        if is_first:
            is_first = False
        else:
            case = "elif"

        msg_type_name = f"""{capitalize(enum_name)}"""
        case_decls0.append(
            f"""    {case} msg.__class__ == {module_name}_pb2.{msg_type_name}:\n"""
            f"""        return ({enum_value}).to_bytes(1, 'little', signed=False) + msg.SerializeToString()"""
        )
        case_decls1.append(
            f"""    {case} msg_type_uint32 == {enum_value}:\n"""
            f"""        msg = {module_name}_pb2.{msg_type_name}()\n"""
            f"""        msg.ParseFromString(encoded_msg[1:])\n"""
            f"""        return msg"""
        )
    case_decls_output0 = "\n".join(case_decls0)
    case_decls_output1 = "\n".join(case_decls1)

    function_name0 = f"""encode_msg"""
    function_defs.append(
        f"""def {function_name0}(msg):\n"""
        f"""{case_decls_output0}\n"""
        f"""    else:\n"""
        f"""      raise TypeError(f"Unknown msg type: {{msg.__class__}}")\n"""
    )
    function_name1 = f"""decode_msg"""
    function_defs.append(
        f"""def {function_name1}(encoded_msg):\n"""
        f"""    msg_type_uint32 = int.from_bytes(encoded_msg[:1], byteorder='little')\n"""
        f"""{case_decls_output1}\n"""
        f"""    else:\n"""
        f"""      raise TypeError(f"Unknown msg type: {{msg_type_uint32}}")\n"""
    )
    function_defs_output = "\n\n".join(function_defs)

    output = \
        f"""{import_decls_output}\n\n""" \
        f"""{function_defs_output}"""

    output_dir = package_name.replace(".", "/")
    output_file_name = f"""{output_dir}/{module_name}_ext.py"""
    with open(output_file_name, "w") as output_file:
        output_file.write(output)


if __name__ == "__main__":
    proto_file, enum_type_name = parse()
    content = proto_file.read().replace("\n", "")
    package_name = re.search(r"package\s+([^;]+);", content).group(1)
    module_name = re.sub(r"([^.]+).proto$", r"\1", basename(proto_file.name))
    enum_pairs = extract(content, enum_type_name)

    output(package_name, module_name, enum_pairs)
