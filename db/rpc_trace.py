from google.appengine.api import apiproxy_stub_map
import logging

__ALL__ = ['register']

def pre_call_hook(service, call, request, response, rpc=None):
    if service.startswith('datastore'):
        if call == 'Put':
            keys = [e.key() for e in request.entity_list()]
            paths = [k.path() for k in keys if not k.has_name_space() or k.name_space()[0] != '_']
            elements = [p.element_list()[-1] for p in paths]
            types = [(e.type(), e.id() if e.has_id() else e.name()) for e in elements]
        
            if len(types) > 0:
                logging.info("appengine rpc: %s %s %s" % (service, call, list(set(types))))
            return
    
        if call in ('Get', 'Delete'):
            paths = [k.path() for k in request.key_list() if not k.has_name_space() or k.name_space()[0] != '_']
            elements = [p.element_list()[-1] for p in paths]
            types = [(e.type(), e.id() if e.has_id() else e.name()) for e in elements]
        
            if len(types) > 0:
                logging.info("appengine rpc: %s %s %s" % (service, call, list(set(types))))
            return
    
        if call in ('RunQuery'):
            if not request.has_name_space() or request.name_space()[0] != '_':
                logging.info("appengine rpc: %s %s %s" % (service, call, request))
            return

    if call not in ('logservice', 'memcache'):
        logging.info("appengine rpc: %s %s %s" % (service, call, request.__class__))

def post_call_hook(service, call, request, response, rpc=None, error=None):
    pass

def register():
    apiproxy_stub_map.apiproxy.GetPreCallHooks().Append('rpctrace', pre_call_hook)
    apiproxy_stub_map.apiproxy.GetPostCallHooks().Append('rpctrace', post_call_hook)
