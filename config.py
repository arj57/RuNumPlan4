import argparse
from typing import Optional, Type
from lxml import etree
from lxml.etree import ElementTree


class Config(object):
    def __init__(self):
        arg_parser = argparse.ArgumentParser(description='Import operators\' owned phone numbers ranges from official site to database')

        arg_parser.add_argument("-c", "--config-file", default="config.xml", help="Config .xml file name")
        arg_parser.add_argument("--dry-run", default=False, nargs="?", const="True",
                                action=self.__overwrite_param("./DryRun"))
        # arg_parser.add_argument("--src-url", default=None, action=self.__overwrite_param("./Src/Url"))
        arg_parser.add_argument("--dst-url", default=None, action=self.__overwrite_param("./Dst/Url"))
        arg_parser.add_argument("-s", "--silence", default=False, nargs="?", const="True",
                                action=self.__overwrite_param("./Logging/Silence"), help="Suppress console output")

        p_args: argparse.Namespace = arg_parser.parse_args()

        self.root_node: ElementTree = etree.parse(p_args.config_file)
        # self._config_validation(self.confTree)

        for key in p_args.__dict__:
            if type(p_args.__dict__[key]) == dict:
                self.root_node.find(p_args.__dict__[key]["path"]).text = p_args.__dict__[key]["value"]

    # https: // docs.python.org / 3.10 / library / typing.html  # typing.Type
    def __overwrite_param(self, path_to_save) -> Type[argparse.Action]:
        class CustomAction(argparse.Action):
            def __call__(self, parser, args, values, option_string=None):
                setattr(args, self.dest, {"path": path_to_save, "value": values})

        return CustomAction

    def get_param_val(self, path: str, context_node: Optional[ElementTree] = None):
        if context_node is None:
            context_node = self.root_node

        node = context_node.find(path)
        if node is None:
            return None

        # Process escape sequences in a string
        # https: // stackoverflow.com / questions / 4020539 / process - escape - sequences - in -a - string - in -python
        res = bytes(node.text, "utf-8").decode("unicode_escape")
        if "type" in context_node.find(path).attrib.keys():
            _type = context_node.find(path).attrib["type"]
            if res == "None":
                return None
            elif _type == "int":
                return int(res)
            elif _type == "bytes":
                return bytes(res, "utf-8")
            elif _type == "bool":
                return bool(res in ['True', 'true', '1', 'y', 'yes'])
            else:
                return res
        else:
            return res

    def findall(self, path: str, context_node: Optional[ElementTree] = None) -> list[ElementTree]:
        if context_node is None:
            context_node = self.root_node

        return context_node.findall(path)

    def find(self, path: str, context_node: Optional[ElementTree] = None) -> ElementTree:
        if context_node is None:
            context_node: ElementTree = self.root_node
        r = context_node.find(path)
        return r

    def xpath(self, abs_path: str):
        return self.root_node.xpath(abs_path)


if __name__ == '__main__':
    c = Config()
    keys = c.get_param_val(r"./DryRun")
    print(keys)
