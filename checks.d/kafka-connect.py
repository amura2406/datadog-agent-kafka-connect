import time
import requests

from checks import AgentCheck
from hashlib import md5

class HTTPCheck(AgentCheck):
    def check(self, instance):
        if 'url' not in instance:
            self.log.info("Skipping instance, no url found.")
            return

        # Load values from the instance configuration
        base_url = instance['url']
        default_timeout = self.init_config.get('default_timeout', 5)
        connector_name = self.init_config.get('connector_name', 'ks')
        url = "%s%s/status" % (base_url, connector_name)
        timeout = float(instance.get('timeout', default_timeout))

        # Use a hash of the URL as an aggregation key
        aggregation_key = md5(url).hexdigest()

        # Check the URL
        start_time = time.time()
        try:
            r = requests.get(url, timeout=timeout)
            end_time = time.time()
        except requests.exceptions.Timeout as e:
            # If there's a timeout
            self.timeout_event(url, timeout, aggregation_key)
            return

        if r.status_code != 200:
            self.status_code_event(url, r, aggregation_key)

        timing = end_time - start_time
        self.success_event(url, r, aggregation_key)

    def timeout_event(self, url, timeout, aggregation_key):
        self.event({
            'timestamp': int(time.time()),
            'event_type': 'kafka_connect_check',
            'msg_title': 'URL timeout',
            'msg_text': '%s timed out after %s seconds.' % (url, timeout),
            'aggregation_key': aggregation_key
        })

    def status_code_event(self, url, r, aggregation_key):
        self.event({
            'timestamp': int(time.time()),
            'event_type': 'kafka_connect_check',
            'msg_title': 'Invalid response code for %s' % url,
            'msg_text': '%s returned a status of %s' % (url, r.status_code),
            'aggregation_key': aggregation_key
        })

    def success_event(self, url, r, aggregation_key):
        connector_name = self.init_config.get('connector_name', 'ks')
        resp_json = r.json()
        task_status = resp_json['tasks'][0]['state']
        alert_type = 'success'
        msg_title = "Kafka connector %s is running" % connector_name
        msg = ""

        if task_status == 'FAILED':
            alert_type = 'error'
            stacktrace = resp_json['tasks'][0]['trace']
            msg_title = "Kafka connector %s has failed." % connector_name
            msg = "Cause:\n%s" % stacktrace
        elif task_status == 'PAUSED':
            alert_type = 'warning'
            msg_title = "Kafka connector %s is paused" % connector_name
        elif task_status == 'UNASSIGNED':
            alert_type = 'warning'
            msg_title = "Kafka connector %s is on unassigned state" % connector_name

        self.event({
            'timestamp': int(time.time()),
            'alert_type': alert_type,
            'event_type': 'kafka_connect_check',
            'msg_title': msg_title,
            'msg_text': msg,
            'aggregation_key': aggregation_key
        })

if __name__ == '__main__':
    check, instances = HTTPCheck.from_yaml('/etc/datadog-agent/conf.d/kafka-connect.yaml')
    for instance in instances:
        print "\nRunning the check against url: %s" % (instance['url'])
        check.check(instance)
        if check.has_events():
            print 'Events: %s' % (check.get_events())
        print 'Metrics: %s' % (check.get_metrics())
