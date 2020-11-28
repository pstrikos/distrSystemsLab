# coding=utf-8
# ------------------------------------------------------------------------------------------------------
# TDA596 - Lab 2
# server/server.py
# Input: Node_ID total_number_of_ID
# Student: Panagiotis Strikos
# ------------------------------------------------------------------------------------------------------
import traceback
import sys
import time
import json
import argparse
from threading import Thread

from bottle import Bottle, run, request, template
import requests
# ------------------------------------------------------------------------------------------------------
try:
    app = Bottle()

    #board stores all message on the system 
    board = {'0' : "Welcome to Distributed Systems Course"} 

    #This functions will add an new element
    def add_new_element_to_store(entry_sequence, element, is_propagated_call=False):
        global board, node_id
        success = False
        try:
           # every key is handled as an str to have a consistency among every function operating on board
           if str(entry_sequence) not in board:
                board[str(entry_sequence)] = element
                success = True
        except Exception as e:
            print e
        return success

    def modify_element_in_store(entry_sequence, modified_element, is_propagated_call = False):
        global board, node_id
        success = False
        try:
            # every key is handled as an str to have a consistency among every function operating on board
            if str(entry_sequence) in board:
                board[str(entry_sequence)] = modified_element
                success = True
        except Exception as e:
            print e
        return success

    def delete_element_from_store(entry_sequence, is_propagated_call = False):
        global board, node_id
        success = False
        try:
            # every key is handled as an str to have a consistency among every function operating on board
            if str(entry_sequence) in board:
                del board[str(entry_sequence)]
                success = True
            # DEBUGGING
            else:
                print str(entry_sequence) + " not in the board, sorry..."
            
        except Exception as e:
            print e
        return success

    # acts on the leader's board and the propagates it the every follower so they can update their copy
    # Regardless of the <action>, we send both new_element and element_id to this function
    # ADD    : the two values contain the place where the new value should be put in the board
    # DELETE : element_id contains the id of the soon to be deleted element. new_element will be None
    # MODIFY : element_id contains the id of the element we will modify, while the new_element holds its new value
    def update_and_propagate(action, element_id, new_element):
        global board

        if (action == "ADD"):
            print "adding new element to leaders board"
            new_id = int(max(board)) + 1 if bool(board) else 0 # assign 0 when the dict is empty
            add_new_element_to_store(new_id, new_element, False)
        elif (action == "DELETE"):
            print "delete element from leader's board"
            delete_element_from_store(element_id, False)
        elif (action == "MODIFY"):
            print "modify element " 
            modify_element_in_store(element_id, new_element, False)

        # propagate new board to other vessels
        thread = Thread(target=propagate_to_vessels,
                        args=('/UPDATE_TABLE', json.dumps(board) , 'POST'))
        thread.daemon = True
        thread.start()


    # ------------------------------------------------------------------------------------------------------
    # ROUTES
    # ------------------------------------------------------------------------------------------------------
    # a single example (index) for get, and one for post
    # ------------------------------------------------------------------------------------------------------
    #No need to modify this
    @app.route('/')
    def index():
        global board, node_id
        return template('server/index.tpl', board_title='Vessel {}'.format(node_id),
                board_dict=sorted({"0":board,}.iteritems()), members_name_string='YOUR NAME')

    @app.get('/board')
    def get_board():
        global board, node_id
        print board
        return template('server/boardcontents_template.tpl',board_title='Vessel {}'.format(node_id), board_dict=sorted(board.iteritems()))

    @app.get('/claim_leadership/<candidate_id>')
    def claim_leader(candidate_id):
        global node_id 
        # print "node_id: " + str(node_id)
        # print "cand_id: " + str(candidate_id)
        if int(node_id) > int(candidate_id):
            elect_new_leader()
            return "denied"
        else:
            return "accepted"
        
    
    #------------------------------------------------------------------------------------------------------
    
    @app.post('/board')
    def client_add_received():
        '''Adds a new element to the board
        Called directly when a user is doing a POST request on /board'''
        global board, node_id, leader_ip, leader_id, unsent_queue, unsent
        try:
            new_entry = request.forms.get('entry')
            payload = {'element_id': '', 'new_element':new_entry}
            path = '/contactLeader/ADD'
            req = 'POST'

            # contact leader with the element that is to be inserted
            try:
                if (node_id == leader_id): # this is the leader trying to act
                    # skip the POST method and act directly
                    update_and_propagate('ADD', '', new_entry)
                else:
                    if (contact_vessel(leader_ip, path, payload, req) != True):
                        unsent_data = {'path': path, 'payload' : payload}
                        unsent = True # there is an unsent action now 
                        unsent_queue = json.dumps(unsent_data)
                        print "Time for Elections"
                        elect_new_leader()
            except Exception as e:
                print e
            return True
        except Exception as e:
            #print e
            pass
        return False

    @app.post('/board/<element_id:int>/')
    def client_action_received(element_id):
        global board, node_id, unsent, unsent_queue
        entry = request.forms.get('entry')
        
        delete_option = request.forms.get('delete') 
	    #0 = modify, 1 = delete
        #call either DELETE of MODIFY base on delete_option value
        if delete_option == str(1): 
            #print "have to delete"
            try:
                if (node_id == leader_id): # this is the leader trying to act
                    update_and_propagate('DELETE', element_id, '')
                else:
                    path = '/contactLeader/DELETE'
                    payload = {'element_id' : element_id, 'new_element' : ''}
                    if (contact_vessel(leader_ip, path, payload, 'POST') != True):
                        unsent_data = {'path': path, 'payload': payload}
                        unsent = True
                        unsent_queue = json.dumps(unsent_data)
                        elect_new_leader()
            except Exception as e:
                pass
        elif delete_option == str(0):
            #print "have to modify"
            try:
                if (node_id == leader_id): # this is the leader trying to act
                    update_and_propagate('MODIFY', element_id, entry)
                else:
                    path = '/contactLeader/MODIFY'
                    payload = {'element_id' : element_id, 'new_element' : entry}
                    if (contact_vessel(leader_ip, path, payload, 'POST') != True):
                        unsent_data = {'path': path, 'payload': payload}
                        unsent = True
                        unsent_queue = json.dumps(unsent_data)
                        elect_new_leader()
                        
            except Exception as e:
                pass


    #With this function you handle requests from other nodes like add modify or delete
    @app.post('/propagate/<action>/<element_id>')
    def propagation_received(action, element_id):
	    #get entry from http body
        entry = request.forms.get('entry')
        print "the action is", action
        
        # Handle requests
        # for example action == "ADD":
        if (action == "ADD"):
            add_new_element_to_store(element_id, entry, True)
        elif (action == "DELETE"):
            delete_element_from_store(element_id, True)
        elif (action == "MODIFY"):
            modify_element_in_store(element_id, entry, True)

    @app.post('/contactLeader/<action>')
    def chating(action):
        global board
        
        new_element = request.forms.get('new_element')
        element_id = request.forms.get('element_id')
        update_and_propagate(action, element_id, new_element)


    @app.post('/elections_completed/<new_leader_id>')
    def update_leader(new_leader_id):
        global leader_id, leader_ip, unsent_queue, unsent
        leader_id = int(new_leader_id)
        leader_ip = '10.1.0.' + str(new_leader_id)
        print "my new leader is " + str(leader_id)
        if (unsent == True):
            unsent = False
            unsent_data = json.loads(unsent_queue)
            contact_vessel(leader_ip, unsent_data['path'], unsent_data['payload'], 'POST')


    # replace the old board in every follower with the updated received from the leader
    @app.post('/UPDATE_TABLE')
    def upd_table():
        global board
        board = json.loads(request.body.read())


    # ------------------------------------------------------------------------------------------------------
    # DISTRIBUTED COMMUNICATIONS FUNCTIONS
    # ------------------------------------------------------------------------------------------------------

    # A follower calls the leader with a request to act and the leader modifies the board 
    # and then distributed its copy to the rest of the follower.
    # There is one extreme case where the vessel trying to act IS the lead. 
    # that way the leader will add it to the dict and the pass it to its followers
    # In that scenario, it "skips" the line and modifies the board directly from here.
    # Otherwise, if the leader uses a POST method to himself, the function will hang.
    def contact_vessel(vessel_ip, path, payload=None, req='POST'):
        success = False
        try:
            if 'POST' in req:
                res = requests.post('http://{}{}'.format(vessel_ip, path), data=payload)
            elif 'GET' in req:
                res = requests.get('http://{}{}'.format(vessel_ip, path))
            else:
                print 'Non implemented feature!'
            # result is in res.text or res.json()
            print(res.text)
            if res.status_code == 200:
                success = True
        except Exception as e:
            pass
            #print e
        return success

    def propagate_to_vessels(path, payload = None, req = 'POST'):
        global vessel_list, node_id

        for vessel_id, vessel_ip in vessel_list.items():
            if int(vessel_id) != node_id: # don't propagate to yourself
                success = contact_vessel(vessel_ip, path, payload, req)
                if not success:
                    print "\n\nCould not contact vessel {}\n\n".format(vessel_id)

    def elect_new_leader():
        global node_id, leader_id, leader_ip
        max_index = True
        imtheone = False
        path = '/claim_leadership/' + str(node_id)
        for vessel_id, vessel_ip in vessel_list.items():
            if int(vessel_id) > int(node_id):
                max_index = False 
                try:
                    response = requests.get('http://{}{}'.format(vessel_ip, path))
                    if (response.text == 'denied'):
                        imtheone = False
                        print "request denied from node " + str(vessel_id)
                        break
                except Exception as e:    
                    #print e
                    imtheone = True

        if max_index == True or imtheone == True:
            print "I AM THE LEADER NOW"
            # assume power
            leader_id = node_id
            leader_ip = '10.1.0.' + str(node_id)
            print "Im propagating to others"

            thread = Thread(target=propagate_to_vessels,
                            args=('/elections_completed/' + str(node_id), None, 'POST'))
            thread.daemon = True
            thread.start()
    # ------------------------------------------------------------------------------------------------------
    # EXECUTION
    # ------------------------------------------------------------------------------------------------------
    def main():
        global vessel_list, node_id, app, leader_id, leader_ip, unsent_queue, unsent
        
        leader_id = -1
        leader_ip = '10.1.0.' + str(leader_id)

        unsent_queue = {}
        unsent = False

        port = 80
        parser = argparse.ArgumentParser(description='Your own implementation of the distributed blackboard')
        parser.add_argument('--id', nargs='?', dest='nid', default=1, type=int, help='This server ID')
        parser.add_argument('--vessels', nargs='?', dest='nbv', default=1, type=int, help='The total number of vessels present in the system')
        args = parser.parse_args()   # Namespace(nbv=6, nid=<node id>)
        node_id = args.nid
        vessel_list = dict()
        # We need to write the other vessels IP, based on the knowledge of their number


        for i in range(1, args.nbv+1):
            vessel_list[str(i)] = '10.1.0.{}'.format(str(i))

        try:
            run(app, host=vessel_list[str(node_id)], port=port)
        except Exception as e:
            print e
    # ------------------------------------------------------------------------------------------------------
    if __name__ == '__main__':
        main()
        
        
except Exception as e:
        traceback.print_exc()
        while True:
            time.sleep(60.)
