import os
import dns.resolver

class ServiceDiscovery:

    @staticmethod
    def get_any_service(service_name):
        return ServiceDiscovery.get_service(service_name)[0]
        
    @staticmethod
    def get_service(service_name):
        service_fqdn = ServiceDiscovery.construct_service_fqdn(service_name)
        answer = dns.resolver.query(service_fqdn, 'SRV')
        return [(a.target.to_text(), a.port) for a in answer]

    @staticmethod
    def construct_service_fqdn(service_name):
        consul_domain = os.getenv('SERVICE_DISCOVERY_DOMAIN', "consul")
        if service_name.endswith('.'):
            return service_name + "service." + consul_domain
        return service_name + ".service." + consul_domain
