# coding=utf-8


class ServiceConfigData(object):

    def __init__(self, **kwargs):
        self.tag = kwargs.get('tag')
        self.weight = kwargs.get('weight')
        self.hostname = kwargs.get('hostname')

    def to_dict(self):
        return {
            'tag': self.tag,
            'weight': self.weight,
            'hostname': self.hostname
        }
