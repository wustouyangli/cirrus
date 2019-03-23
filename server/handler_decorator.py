# coding=utf-8

import inspect
import functools
import logging

logger = logging.getLogger(__name__)


class HandlerDecoratorMetaclass(type):
    def __new__(mcs, class_name, class_bases, class_dict):
        handler_class = class_bases[0]

        def __init__(self):
            pass

        class_dict['__init__'] = __init__

        def response(f):
            @functools.wraps(f)
            def wrapper(self, *args, **kwargs):
                res = f(self, *args, **kwargs)
                param_list = []
                for arg in args:
                    param_list.append(str(arg))
                for key, value in kwargs.items():
                    param_list.append('%s=%s' % (key, value))
                param_str = ', '.join(param_list) if len(param_list) else ''

                logger.info('Handler request: %s(%s), return result: %s', f.__name__, param_str, str(res))
                return res

            return wrapper

        for method_name, method in inspect.getmembers(handler_class, predicate=inspect.ismethod):
            if method_name.startswith('__'):
                continue
            attr = getattr(handler_class, method_name, None)
            if attr is None or not inspect.ismethod(attr):
                continue
            class_dict[method_name] = response(method)

        return type.__new__(mcs, class_name, class_bases, class_dict)


class HandlerDecorator(object):

    __metaclass__ = HandlerDecoratorMetaclass
