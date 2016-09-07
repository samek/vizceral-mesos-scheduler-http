import pycurl
from StringIO import StringIO
import sys
import json
import logging
from io import BytesIO
import uuid
import base64

from mesos_proto import scheduler_pb2 as sched_pb2
from mesos_proto import mesos_pb2 as mes_pb2


logging.basicConfig(level=logging.INFO)


class VizceralScheduler:
    master_host="http://master.mesos:5050"
    master_host="http://172.22.61.59:5050"
    curl_header = ['Content-Type: application/json']
    curl_header_with_stream_id = curl_header
    mesos_stream_id=""
    mesos_framework_id=""
    DOCKER_JSON = "./resources/container.json"
    LAUNCH_JSON = "./resources/launch.json"
    TASK_RESOURCES_JSON = "./resources/task_resources.json"
    ACKNOWLEDGE_JSON    = "./resources/acknowledge.json"
    TASKS_PACKETBEAT_AGENTS=[]
    TASK_VIZCERAL_DATA={}
    TASK_VIZCERAL_UI={}
    data_buffer = ''
    data_size   = 0




    def __init__(self):

        pass

    def check_task_packetbeat_running_on_agent(self,agent_id,return_task=False):
       for task in self.TASKS_PACKETBEAT_AGENTS:
           task_agent_id=task['agent_id']
           task_task_id = task['task_id']
           if agent_id == task_agent_id:
               if return_task:
                    return task
               else:
                   return True

       ## Did not found it#
       return False

    def insert_task_packetbeat(self,agent_id,task_id):
        ##check if it's inserted..
        if self.check_task_packetbeat_running_on_agent(agent_id):
            return False
        self.TASKS_PACKETBEAT_AGENTS.append({'agent_id':agent_id,'task_id':task_id})
        return True

    def remove_task_packetbeat(self,agent_id):
        ##check if it's in
        task = self.check_task_packetbeat_running_on_agent(agent_id,True)
        if task:
            self.TASKS_PACKETBEAT_AGENTS.remove(task)
            return True
        else:
            return False



    def get_json(self,filename):
        """ Loads the JSON from the given filename."""
        with open(filename) as jsonfile:
            lines = jsonfile.readlines()

        return json.loads("".join(lines))


    def send_call(self,json):
        buff = BytesIO()
        logging.info("Call: %s", json)
        c = pycurl.Curl()

        c.setopt(pycurl.URL, self.master_host + "/api/v1/scheduler")
        c.setopt(pycurl.POST, True)
        c.setopt(pycurl.WRITEHEADER, buff)
        c.setopt(pycurl.FOLLOWLOCATION, True)
        c.setopt(pycurl.HTTPHEADER, self.curl_header_with_stream_id)
        c.setopt(pycurl.POSTFIELDS,json)
        #c.setopt(pycurl.VERBOSE,True)
        c.perform()
        response = buff.getvalue()
        if response.find('202 Accepted')!=-1:
            logging.info("Call accepted")
            return True
        else:
            logging.info("Call Error: %s",response)
            return False

    def register_framework(self):

        c = pycurl.Curl()
        c.setopt(pycurl.URL, self.master_host+"/api/v1/scheduler")
        c.setopt(pycurl.WRITEFUNCTION, self.process_event)
        c.setopt(pycurl.HEADERFUNCTION, self.process_header)
        c.setopt(pycurl.POST, True)
        c.setopt(pycurl.FOLLOWLOCATION, True)
        c.setopt(pycurl.HTTPHEADER, self.curl_header)
        c.setopt(pycurl.POSTFIELDS,
                 '{"type":"SUBSCRIBE","subscribe":{"framework_info":{"user":"root","name":"Example HTTP Framework"}}}')
        c.perform()

    ## Callback function invoked when body data is ready
    def process_event(self,buf):
        if buf.find("\n")!=-1:
            data = buf.split('\n')
            self.data_buffer = data[1]
            self.data_size = int(data[0])
        else:
            self.data_buffer=self.data_buffer+buf

        logging.info("Current data buffer %s size %d", self.data_buffer,self.data_size)
        if len(self.data_buffer)==self.data_size:
            data = self.data_buffer;
            self.data_buffer=''
            self.data_size=0

            logging.info("Event: %s",data)
            data = json.loads(data)
            ##we save the framework id##
            if data['type'] == 'SUBSCRIBED':
                framework_id = data['subscribed']['framework_id']
                self.mesos_framework_id = framework_id['value']
                self.curl_header_with_stream_id.append('Mesos-Stream-Id: '+self.mesos_stream_id)
                #print json.dumps(self.curl_header_with_stream_id)

            if data['type'] == 'OFFERS':
                self.check_offers(data['offers']['offers'])
            if data['type'] == 'UPDATE':
                base64data = data['update']['status']['data']
                #logging.info("DATA DECODED %s"+base64.b64decode(base64data))
                agent_id = data['update']['status']['agent_id']['value']
                task_id  = data['update']['status']['task_id']['value']
                uuid     = data['update']['status']['uuid']
                state = data['update']['status']['state']
                if state=='TASK_FINISHED' or state=='TASK_KILLED' or state=='TASK_FAILED':
                    #we need to remove it
                    self.remove_task_packetbeat(agent_id)
                    self.send_acknowledge(uuid,agent_id,task_id)
                pass


            # Returning None implies that all bytes were written



    def send_acknowledge(self,uuid,agent_id,task_id):
        logging.info("ACK send uuid: %s agent_id: %s task_id:%s", uuid,task_id,agent_id)
        ack_json = self.get_json(self.ACKNOWLEDGE_JSON)
        ack_json["framework_id"]["value"] = self.mesos_framework_id
        ack_json["acknowledge"]["agent_id"]["value"] = agent_id
        ack_json["acknowledge"]["task_id"]["value"] = task_id
        ack_json["acknowledge"]["uuid"] = uuid
        self.send_call(json.dumps(ack_json))



    def check_offers(self,offers):
        logging.info("Checking offers: %s", offers)
        #self.decline_offers(offers)

        decline_offers=[]

        for offer in offers:
            agent_id = offer['agent_id']['value']
            offer_id = offer['id']['value']
            task_id  = str(uuid.uuid4())
            #TODO check for resources#

            # check if we have packetbeat running on this agent
            if not self.check_task_packetbeat_running_on_agent(agent_id):
                # we need to start it
                self.accept_offer(task_id,offer_id,agent_id,'Agent')
            else:
                decline_offers.append(offer)

        self.decline_offers(decline_offers)
        pass





    def decline_offers(self,offers):
        decline_obj = self.get_skeleton_callback_object('DECLINE')
        decline_obj['decline']=self.get_skeleton_decline_object_part()
        #print json.dumps(decline_obj)
        for offer in offers:
            if 'id' in offer:
                decline_obj['decline']['offer_ids'].append(offer['id'])


        ### send callback ##
        self.send_call(json.dumps(decline_obj))


    def accept_offer(self,task_id,offer_id,agent_id,type='Agent'):

        if type=="Agent":
            task_name = 'packetbeat_agent'
            cpu = 0.1
            mem = 20
            docker_image = 'nginx:1.10-alpine'
        elif type=="DATA":
            task_name = "vizceral-data"
            cpu = 0.1
            mem = 200
            docker_image = 'nginx:1.10-alpine'
        else:
            task_name = "vizceral-ui"
            cpu = 0.1
            mem = 300
            docker_image = 'nginx:1.10-alpine'



        container_launch_info = self.get_json(self.DOCKER_JSON)
        container_resources = self.get_json(self.TASK_RESOURCES_JSON)
        container_resources[0]['scalar']={'value':cpu}
        container_resources[1]['scalar'] = {'value': mem}

        container_launch_info["framework_id"]["value"] = self.mesos_framework_id
        container_launch_info["accept"]["offer_ids"].append({'value':offer_id})
        container_launch_info["accept"]["operations"][0]["launch"]["task_infos"][0]["name"] = task_name
        container_launch_info["accept"]["operations"][0]["launch"]["task_infos"][0]["container"]["docker"]["image"]=docker_image
        container_launch_info["accept"]["operations"][0]["launch"]["task_infos"][0]["resources"].append(container_resources)
        container_launch_info["accept"]["operations"][0]["launch"]["task_infos"][0]["agent_id"]["value"]=agent_id
        container_launch_info["accept"]["operations"][0]["launch"]["task_infos"][0]["task_id"]["value"] = task_id
        #print (json.dumps(container_launch_info))
        self.insert_task_packetbeat(agent_id,task_id)
        self.send_call(json.dumps(container_launch_info))

        pass



    def get_skeleton_decline_object_part(self):
        obj={'offer_ids':[]}
        return obj

    def get_skeleton_callback_object(self, type='DECLINE'):
        obj = {}
        obj['framework_id'] = {'value': self.mesos_framework_id }
        obj['type'] = type
        return obj

    ## Callback function invoked when header data is ready
    def process_header(self,buf):
        # Print header data to stderr
        #sys.stderr.write(buf)

        headers= buf.split("\r\n")
        for h in headers:
            if h == "":
                continue
            logging.info("Header: %s", h)
            if h.find('Mesos-Stream-Id:')==0:
                h = h.split(":")
                self.mesos_stream_id = h[1]



        # Returning None implies that all bytes were written


fw = VizceralScheduler()
fw.register_framework()