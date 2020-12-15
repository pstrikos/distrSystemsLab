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

    def update_clocks(received_clocks):
        global clocks
        print "++++++++++++++++++++++++++++++++++++++"
        print "clock before receiving: "
        print clocks
        print "received clock:         "
        print eval(json.dumps(received_clocks))
        new_is_bigger = False
        old_is_bigger = False
        for i in range (1,7):
            if received_clocks[str(i)] >= clocks[i]:
                new_is_bigger = True
            else:
                old_is_bigger = True

            if i != node_id:
                clocks[i] = max(clocks[i], received_clocks[str(i)])

        print "new clocks:             " 
        print clocks

        if new_is_bigger and old_is_bigger:
            print "Wrong order..."
        elif new_is_bigger:
            print "Correct order!!"
        else:
            print "Wrong order..."
        print "++++++++++++++++++++++++++++++++++++++"


            

    # ------------------------------------------------------------------------------------------------------
    # ROUTES
    # ------------------------------------------------------------------------------------------------------
    # a single example (index) for get, and one for post
    # ------------------------------------------------------------------------------------------------------
    #No need to modify this
    @app.route('/')
    def index():
        global board, node_id

        #for key in sorted(board.keys()):
        
        # the key is an str so sorting will cause it to be inaccureate (ex '101' < '2')
        # so it needs to be turned into int, get sorted, and then turned into an str again
        i = 0
        sorted_board = {}
        for int_key in sorted([int(key) for key in board.keys()]):
            sorted_board[str(i)] = board[str(int_key)]
            i += 1

        #sorted_board = {str(int_key):board[str(int_key)] for int_key in [int_key for int_key in sorted([int(key) for key in board.keys()])]}

        print "+++++++++++++++++++++++++++++++++++++++"
        print "sorted:"
        print sorted_board
        print "unsorted:"
        print board
        print "+++++++++++++++++++++++++++++++++++++++"
        return template('server/index.tpl', board_title='Vessel {}'.format(node_id),
                board_dict=sorted({"0":sorted_board,}.iteritems()), members_name_string='YOUR NAME')

    @app.get('/board')
    def get_board():
        global board, node_id

        # the key is an str so sorting will cause it to be inaccureate (ex '101' < '2')
        # so it needs to be turned into int, get sorted, and then turned into an str again
        i = 0
        sorted_board = {}
        for int_key in sorted([int(key) for key in board.keys()]):
            sorted_board[str(i)] = board[str(int_key)]
            i += 1

        print "+++++++++++++++++++++++++++++++++++++++"
        print "sorted:"
        print sorted_board
        print "unsorted:"
        print board
        print "+++++++++++++++++++++++++++++++++++++++"
        return template('server/boardcontents_template.tpl',board_title='Vessel {}'.format(node_id), board_dict=sorted(sorted_board.iteritems()))


    # respond to another vessel claiming leadership
    @app.get('/claim_leadership/<candidate_id>')
    def claim_leader(candidate_id):
        global node_id 
        if int(node_id) > int(candidate_id):
            elect_new_leader()
            return "denied"
        else:
            return "accepted"
        
    
    #------------------------------------------------------------------------------------------------------
    
    # If leader wants to perform an action. The board is updated directly and propagated to other vessels
    # If follower wants to act, it tries to contact the leader to take care of the request for him. 
    # When the leader is not there, the vessel initiates elections and stores the data for later use
    @app.post('/board')
    def client_add_received():
        '''Adds a new element to the board
        Called directly when a user is doing a POST request on /board'''
        global board, node_id, leader_ip, leader_id, unsent_queue, unsent, clocks
        try:
            new_entry = request.forms.get('entry')
            # in ADDition, each node has to calculate the new_id on its own.
            # otherwise new_ids might overlap 
            new_id = str(lclock) + str(node_id)
            payload = {'element_id': new_id, 'new_element':new_entry}
            path = '/propagate/ADD/' + str(node_id)

            print "adding new element to myself at clock " + str(lclock)
            add_new_element_to_store(new_id, new_entry, False)

            thread = Thread(target=propagate_to_vessels,
                            args=(path, json.dumps(payload) , 'POST'))
            thread.daemon = True
            thread.start()

        except Exception as e:
            #print e
            pass
        return False

    # same procedure as in ADD
    # all three actions use the same two elements in their payload: new element and element id
    # Not everything is always used
    @app.post('/board/<element_id:int>/')
    def client_action_received(element_id):
        global board, node_id, unsent, unsent_queue
        entry = request.forms.get('entry')
        
        delete_option = request.forms.get('delete') 
	    #0 = modify, 1 = delete
        #call either DELETE of MODIFY base on delete_option value
        if delete_option == str(1): 
            action = 'DELETE'
            new_element = ''
            path = '/contactLeader/DELETE'
        elif delete_option == str(0):
            action = 'MODIFY'
            new_element = entry
            path = '/contactLeader/MODIFY'

        try:
            if (node_id == leader_id): # this is the leader trying to act
                update_and_propagate(action, element_id, new_element)
            else:
                payload = {'element_id' : element_id, 'new_element' : new_element}
                if (contact_vessel(leader_ip, path, payload, 'POST') != True):
                    unsent_data = {'path': path, 'payload': payload}
                    unsent = True
                    unsent_queue = json.dumps(unsent_data)
                    print "Time for Elections"
                    elect_new_leader()
        except Exception as e:
            pass

    @app.post('/propagate/<action>/<element_id>')
    def propagation_received(action, element_id):
        global lclock
        print "received data from node " + str(element_id)

        received_data = json.loads(request.body.read())
        received_lclock = received_data['lclock']
        received_payload = received_data['payload']
        received_new_id = json.loads(received_payload)['element_id']
        received_new_element = json.loads(received_payload)['new_element']

        
        lclock = max(lclock, received_lclock) + 1 #update my clock since I received something

        if action == "ADD":
            print "adding received element at clock " + str(lclock) + " with id " + str(received_new_id)
            add_new_element_to_store(received_new_id, received_new_element)
        else:
            print "operation {} is not defined".format(action)

    # receives a request from a follower and sends it to the leader
    # This function was needed to separate the actions that take place inside "update_and_propagate"
    # That way it was possible to differentiate the action of a leader and a follower
    @app.post('/contactLeader/<action>')
    def chating(action):
        global board
        
        new_element = request.forms.get('new_element')
        element_id = request.forms.get('element_id')
        update_and_propagate(action, element_id, new_element)


    # after the eletion is finished, the leader sends its dissision to every node
    # here, the global variables regarding the leader are updated
    # In case the are unsent data left, a new contact to the leader is attempted
    # CAREFUL: if the leader fails DURING election, there is a chance for the action to not be performed
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
        global vessel_list, node_id, clocks, lclock
        
        # the clock has to be increased once in EVERY iteration
        # since every step of the loop is a new action
        for vessel_id, vessel_ip in vessel_list.items():
            if int(vessel_id) != node_id: # don't propagate to yourself
                lclock += 1
                print "updated my clock to " + str(lclock) + " and I'll message node " + str(vessel_id) 
                send_data = {'lclock' : lclock, 'payload' : payload}
                success = contact_vessel(vessel_ip, path, json.dumps(send_data), req)
                if not success:
                    print "\n\nCould not contact vessel {}\n\n".format(vessel_id)

    # Performs the process of voting a new leader using the bully algortihm
    # Two flags are used
    #   max_index: indicates that the vessel has the maximum id
    #   imtheone:  indicates that a vessel's request to become leader wasn't denied by anyone
    # In both cases the vessel becomes the new leader and shares its decesion 
    #
    # Worst case: vessel 1 initiates voting. 1 contacts 2 and then stops, 2 contacts 3 and stops, and so on
    # Complexity: O(n) where n is the number of vessels

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
        global vessel_list, node_id, app, leader_id, leader_ip, unsent_queue, unsent, clocks, lclock
        #board_with_clocks
        # vector clocks
        clocks = {1:0, 2:0, 3:0, 4:0, 5:0, 6:0}
        lclock = 1 # logic clock for this node, starts from 1 since there is already an element in the board when starting

        # initiate to a non-existant index
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


        #elem_id = '0' + str(node_id)
        #board_with_clocks = {elem_id : board['0']}

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
