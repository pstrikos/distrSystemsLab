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
    # the time that an action occured to an element of the board.
    # <element_id> : <clock>}
    # where <clock> is the clock of the node that made the change 
    board_timing = {'0': '0'} 

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
            #print e
            # in case the element has been deleted by other node
            print "element {} doent exist in the board".format(entry_sequence)
        return success

    def delete_element_from_store(entry_sequence, is_propagated_call = False):
        global board, node_id
        success = False
        try:
            # every key is handled as an str to have a consistency among every function operating on board
            if str(entry_sequence) in board:
                del board[str(entry_sequence)]
                success = True
            else:
                print str(entry_sequence) + " not in the board, sorry..."
            
        except Exception as e:
            #print e
            print "Element {} doesnt exist in the board".format(entry_sequence)
        return success


    # ------------------------------------------------------------------------------------------------------
    # ROUTES
    # ------------------------------------------------------------------------------------------------------
    # a single example (index) for get, and one for post
    # ------------------------------------------------------------------------------------------------------
    #No need to modify this
    @app.route('/')
    def index():
        global board, node_id

        
        # the key is an str so sorting it will create errors (ex '101' < '2' when we're dealing with strings)
        # so it needs to be turned into int, get sorted, and then turned into an str again

        # Also I tried using a new key for sorted_board which starts from 0 and increments by one
        # Although its nicer having keys 1, 2, 3 ... in the board, it later causes a problem
        # when modifying/deleting an element
        # When we request an action like that, we get the new key instead of the real one
        sorted_board = {}
        for int_key in sorted([int(key) for key in board.keys()]):
            sorted_board[str(int_key)] = board[str(int_key)]

        print sorted_board

        return template('server/index.tpl', board_title='Vessel {}'.format(node_id),
                board_dict=sorted({"0":sorted_board,}.iteritems()), members_name_string='YOUR NAME')

    @app.get('/board')
    def get_board():
        global board, node_id

        sorted_board = {}
        for int_key in sorted([int(key) for key in board.keys()]):
            sorted_board[str(int_key)] = board[str(int_key)]

        print sorted_board

        return template('server/boardcontents_template.tpl',board_title='Vessel {}'.format(node_id), board_dict=sorted(sorted_board.iteritems()))

    #------------------------------------------------------------------------------------------------------
    
    @app.post('/board')
    def client_add_received():
        '''Adds a new element to the board
        Called directly when a user is doing a POST request on /board'''
        global board, node_id, leader_ip, leader_id, unsent_queue, unsent, clocks, board_timing
        try:
            new_entry = request.forms.get('entry')
            # in ADDition, each node has to calculate the new_id on its own.
            new_id = str(lclock) + str(node_id)
            payload = {'sender_clk' : str(lclock), 'element_id': '', 'new_element':new_entry}
            path = '/propagate/ADD/' + str(node_id)

            print "adding new element to myself at clock " + str(lclock) # debugging
            if add_new_element_to_store(new_id, new_entry, False):
                board_timing[str(new_id)] = str(new_id)
                print board_timing # debugging

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
    # The nodes first modify their own board and board_timing and then they propagate those changes to their peers
    #
    # When a node creates its payload, it attached the clock during which it changed the board for the FIRST time
    # This way everyone knows the chronological order of every modification 
    @app.post('/board/<element_id:int>/')
    def client_action_received(element_id):
        global board, node_id, unsent, unsent_queue, clocks
        try:
            entry = request.forms.get('entry')
            
            delete_option = request.forms.get('delete') 
            # DELETE
            if delete_option == str(1): 
                new_element = ''
                path = '/propagate/DELETE/' + str(node_id)
                print "deleting element " + str(element_id) +  " for myself at clock " + str(lclock)
                delete_element_from_store(element_id)
                del board_timing[str(element_id)]
                print board_timing

            # MODIFY
            elif delete_option == str(0):
                new_element = entry
                path = '/propagate/MODIFY/' + str(node_id)
                print "modifying element " + str(element_id) + " for myself at clock " + str(lclock)
                modify_element_in_store(element_id, new_element)
                board_timing[str(element_id)] = str(lclock) + str(node_id)
                print board_timing

            # this clock will be part of every message sent from this node
            payload = {'sender_clk' : str(lclock), 'element_id' : str(element_id), 'new_element' : new_element}

            thread = Thread(target=propagate_to_vessels,
                            args=(path, json.dumps(payload) , 'POST'))
            thread.daemon = True
            thread.start()
        except Exception as e:
            pass

    @app.post('/propagate/<action>/<element_id>')
    def propagation_received(action, element_id):
        global lclock, board, board_timing
        print "received data from node " + str(element_id)

        received_data = json.loads(request.body.read())
        received_lclock = received_data['lclock']
        received_payload = received_data['payload']
        received_sender_clk =json.loads(received_payload)['sender_clk'] 
        received_id = json.loads(received_payload)['element_id']
        received_new_element = json.loads(received_payload)['new_element']

        #update my clock since I received something
        lclock = max(lclock, received_lclock) + 1 

        # Two cases for an addition request
        # 1) The element's id IS NOT in the board_timing which means that no one has added this element before
        #    We just add it to board and create a key for board timing
        # 2) The element's id is already IS in board_timing, but NOT is board, so the node received a deletion request
        #    before the first add. In that case we do nothingh
        # 3) The element's id IS in board_timing AND board meaning that the node received a modification request
        #    earlier than when it was supposed to and has already added the element. We do nothing in that case
        # Note: Since the combination of local clock and node_id are unique, only ONE node at a SPECIFIC time can create
        # an id, so each element's id IS UNIQUE
        if action == "ADD":
            #print "adding received element at clock " + str(lclock) + " with id " + str(received_id)
            # the new id is created so that conflicts between nodes are won from node with biggest id
            new_id = received_sender_clk + str(element_id)
            if received_id not in board_timing: 
                add_new_element_to_store(new_id, received_new_element)
                board_timing[str(new_id)] = new_id
                print board_timing
        # three cases for a modification request
        # 1) there is nothing in the board_timing: this means that we received the request before adding the element
        #    so we modify it anyway and add a new record to board_timing
        # 2) the id is in the board_timing but not in board: this means that someone deleted already the element from the board
        #    so we check WHEN was this done
        # 3) the element can be found in both dicts, so its added before and its still there. 
        #    In that case check the board_timing record again
        elif action == "MODIFY":
            #print "modifying received element at clock " + str(lclock) + " with id " + str(received_id)
            # same as in addition, the new ad (if accepted) will combine the timing with the sender's id to resolve arguments
            new_element_id = received_sender_clk + str(element_id)
            
            if received_id in board_timing:
                if received_id in board:
                    if int(new_element_id) > int(board_timing[received_id]):
                        modify_element_in_store(received_id, received_new_element)
                        board_timing[str(received_id)] = new_element_id
                else:
                    if int(new_element_id) > int(board_timing[received_id]):
                        # modify happend later that what delete the element from the board -> add it again
                        add_new_element_to_store(new_element_id, received_new_element)
                        board_timing[str(received_id)] = new_element_id
                print board_timing
            else: # we received a modif request for an element that hasn't been added yet
                # add the element to the board
                add_new_element_to_store(new_element_id, received_new_element)
                # add a new entry on the board so it can be used when the element arrives
                board_timing[str(received_id)] = new_element_id
                
        # Two cases for a deletion request
        # 1) The element is in the board_timing, in which case we just delete it
        #    It doesnt matter if the DEL request was the last in order. Even if it wasnt (example DEL and then MOD)
        #    the delete request would have later made the element not available to begin with.
        # 2) The element is NOT in the board_timing which means that we received a del request before receiving the add
        #    We do not change the board (its a deletion, remember?) but we update the board_timing dict
        #    That way when the addition arrives later and the node checks the record, it will not add again the element
        elif action == "DELETE":
            deleted_id = received_sender_clk + str(element_id)
            if received_id in board_timing:
                delete_element_from_store(received_id)
                board_timing[str(received_id)] = deleted_id # don't delete the entry, just update it
                print board_timing
            else: # we received a delete for an element that hasn't been added yet
                # add a new entry on the board so it can be used when the element arrives
                board_timing[str(received_id)] = deleted_id # don't delete the entry, just update it
        else:
            print "operation {} is not defined".format(action)

    # ------------------------------------------------------------------------------------------------------
    # DISTRIBUTED COMMUNICATIONS FUNCTIONS
    # ------------------------------------------------------------------------------------------------------

    def contact_vessel(vessel_ip, path, payload=None, req='POST'):
        success = False
        try:
            if 'POST' in req:
                res = requests.post('http://{}{}'.format(vessel_ip, path), data=payload)
            elif 'GET' in req:
                res = requests.get('http://{}{}'.format(vessel_ip, path))
            else:
                print 'Non implemented feature!'
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
                #print "updated my clock to " + str(lclock) + " and I'll message node " + str(vessel_id) # debugging
                send_data = {'lclock' : lclock, 'payload' : payload}
                success = contact_vessel(vessel_ip, path, json.dumps(send_data), req)
                if not success:
                    print "\n\nCould not contact vessel {}\n\n".format(vessel_id)
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
