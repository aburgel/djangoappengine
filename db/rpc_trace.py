from google.appengine.api import apiproxy_stub_map
import logging

__ALL__ = ['register']

def pre_trace_datastore_v3(service, call, request, response, rpc):
    if call == 'Put':
        keys = [e.key() for e in request.entity_list()]
        paths = [k.path() for k in keys if not k.has_name_space() or k.name_space()[0] != '_']
        elements = [p.element_list()[-1] for p in paths]
        types = [(e.type(), e.id() if e.has_id() else e.name()) for e in elements]
    
        if len(types) > 0:
            logging.info("%s: %s %s" % (service, call, list(set(types))))
        return

    if call in ('Get', 'Delete'):
        paths = [k.path() for k in request.key_list() if not k.has_name_space() or k.name_space()[0] != '_']
        elements = [p.element_list()[-1] for p in paths]
        types = [(e.type(), e.id() if e.has_id() else e.name()) for e in elements]
    
        if len(types) > 0:
            logging.info("%s: %s %s" % (service, call, list(set(types))))
        return

    if call in ('RunQuery'):
        if not request.has_name_space() or request.name_space()[0] != '_':
            logging.info("%s: %s %s" % (service, call, request))
        return

def post_trace_memcache(service, call, request, response, rpc, error):
    if call == 'Get':
        keys = request.key_list()

        logging.info("%s: %s %s %s" % (service, call, keys, len(response.item_list())))
        return
    
    if call == 'Set':
        keys = [i.key() for i in request.item_list()]
        logging.info("%s: %s %s" % (service, call, keys))
        return
    
    logging.info("%s: %s %s" % (service, call, request.__class__))

def create_pre_call_hook(enabled):
    def pre_call_hook(service, call, request, response, rpc=None):
        if service in enabled:
            method = "pre_trace_%s" % service
            func = globals().get(method, None)
            if func:
                func(service, call, request, response, rpc)
    return pre_call_hook

def create_post_call_hook(enabled):
    def post_call_hook(service, call, request, response, rpc=None, error=None):
        if service in enabled:
            method = "post_trace_%s" % service
            func = globals().get(method, None)
            if func:
                func(service, call, request, response, rpc, error)
    return post_call_hook

def register(*services):
    apiproxy_stub_map.apiproxy.GetPreCallHooks().Append('rpctrace', create_pre_call_hook(services))
    apiproxy_stub_map.apiproxy.GetPostCallHooks().Append('rpctrace', create_post_call_hook(services))
