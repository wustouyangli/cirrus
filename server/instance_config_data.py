# coding=utf-8


class InstanceConfigData(object):

    def __init__(self, **kwargs):
        self.tag = kwargs.get('tag')
        self.weight = kwargs.get('weight', 100)
        self.hostname = kwargs.get('hostname', 'unknown')

    def to_dict(self):
        return {
            'tag': self.tag,
            'weight': self.weight,
            'hostname': self.hostname
        }


class InstanceConfigDataExtension(object):

    def __init__(self, **kwargs):
        self.host = kwargs.get('host')
        instance_config_data = kwargs.get('instance_config_data')
        if isinstance(instance_config_data, InstanceConfigData):
            self.instance_config_data = instance_config_data
        else:
            raise Exception('service config data must be InstanceConfigData class')
        self.mtime = kwargs.get('mtime')

    def to_dict(self):
        return {
            'host': self.host,
            'instance_config_data': self.instance_config_data.to_dict(),
            'mtime': self.mtime
        }

