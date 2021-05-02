# coding = utf-8
from socket import *
import hashlib
import math
import os
from os.path import join
from os.path import isfile
import struct
import multiprocessing as mp
from multiprocessing import Process
import threading
import time
import argparse
from collections import ChainMap
import json
import gzip
from tqdm import tqdm

# Message message_type
HELLO = 1
NEWS = 2
FILE = 3
END = 4
MODIFY = 5
# Constant variable
BUFFER_SIZE = 2 * 1024 * 1024
BLOCK_SIZE = 1024 * 1024
FILE_DIR = 'share/'
PORT = 22450
# Global  variable
early_ticket = {}
end_addr = 'no'
IP_ADDRESS1 = ''
IP_ADDRESS2 = ''


################## GENERAL FUNCTION ##################
def mk_file(file_name, text=None):  # make file under log folder, with input name and text
    log_dir = 'log/'
    folder = os.path.exists(log_dir)
    if not folder:
        os.makedirs(log_dir)
    f = open(log_dir + file_name, 'w')
    if text is not None:
        f.write(text)
    f.close()


def read_file(file_name):   # return data of file
    f = open(file_name)
    file_data = f.read()
    f.close()
    return file_data


def get_md5(file):  # return md5 of file with input filepath in string
    file_md5 = hashlib.md5()
    with open(file, 'rb') as f:
        for text in f:
            file_md5.update(text)
    return file_md5.hexdigest()


def compress_file(file_path):       # compressing input file, return the file path for compressed file
    f = open(file_path, 'rb')
    f_compress = gzip.open((file_path + '.gz'), 'wb')
    f_compress.writelines(f_in)
    f_compress.close()
    f.close()
    return file_path + '.gz'


def decompress_file(file_path):
    file_path = comp_file_name[:-3]
    f_compress = gzip.open(comp_file_name, "rb")
    f = open(file_path, "wb")
    content = f_gzip.read()
    f.write(content)
    f.close()
    f_compress.close()


def scan_file(FILE_DIR):    # traverse the folder, input file path and return file information in dictionary {filename: [size, md5]}
    out_dict = {}
    file_list = os.listdir(FILE_DIR)
    while len(os.listdir('log/')) != 0:
        time.sleep(1)
    for i in range(0, len(file_list)):
        file_path = FILE_DIR + file_list[i]
        if isfile(file_path):
            size = os.path.getsize(file_path)
            if size > 1024 * 1024 * 500:    # tiny core cannot calculates md5 exceed 500MB
                md5 = 'toolarge'
            else:
                md5 = get_md5(file_path)
            out_dict[file_path] = [size, md5]
        else:
            in_dict = scan_file(FILE_DIR + file_list[i] + '/')
            out_dict = dict(ChainMap(out_dict, in_dict))
    return out_dict


def compare_file(old_dict, new_dict):   # comparing elements in two dictionary, return dictionary of different element
    dict_diff = {}
    for j in new_dict:
        count = 0
        for i in old_dict:
            if i == j:      # same element
                a = old_dict.get(i)
                b = new_dict.get(j)
                if a == b:
                    break
                elif a[0] > b[0]:   # broken file, ignore
                    break
                else:
                    dict_diff[j] = b    # modified file
                    break
            count += 1
        if count == len(old_dict):      # new file
            dict_diff[j] = new_dict.get(j)
    return dict_diff


def make_package_info(message_type, diction, block_index=None):     # input type, dictionary and block index(can be none), return a packaged message
    send_dict = json.dumps(dict(diction))
    header = struct.pack("!I", message_type)
    if block_index is not None:
        header = struct.pack("!II", message_type, block_index)
    length_header = len(header + send_dict.encode())
    return struct.pack("!I", length_header) + header + send_dict.encode()


def get_file_block(filename, block_index):  # input filename and block index, return a packaged file block
    global BLOCK_SIZE
    f = open(filename, 'rb')
    f.seek(block_index * BLOCK_SIZE)
    file_block = f.read(BLOCK_SIZE)
    f.close()
    block_length = len(file_block)
    return struct.pack("!I", block_length) + file_block


################## SCANNER ##################
def detect_send_change(client_socket, share_dict):
    while True:
        try:
            new_dict = scan_file(FILE_DIR)
            force = dict(share_dict)
            temp_dict = compare_file(force, new_dict)       # detect change
            if temp_dict != {}:
                # if mod == MODIFY:
                #     client_info = make_package_info(MODIFY, new_dict)
                #     client_socket.sendall(client_info)
                #     print('change list send\n')
                client_info = make_package_info(NEWS, new_dict)     # if change, send NEWS
                client_socket.sendall(client_info)
                print('change list send\n')
            share_dict = new_dict
            time.sleep(1)

        except:
            print('old scanner connection closed')
            break


################## DOWNLOADER ##################
def downloader(c_socket, server_name, share_dict):
    while True:
        try:
            if os.path.exists('log/ticket ' + server_name + '.txt'):
                req_dict = read_file('log/ticket ' + server_name + '.txt')
                req_dict = json.loads(req_dict)

                for f in req_dict:

                    if not os.path.exists('log/log ' + server_name + '.txt'):   # log to write how much file block received
                        log = open('log/log ' + server_name + '.txt', 'w')
                    else:
                        log = open('log/log ' + server_name + '.txt', 'a')

                    share = f.split('/')[-2]    # judge weather there is another folder
                    if share != 'share':
                        sub_dir = 'share/' + share + '/'
                        if not os.path.exists(sub_dir):
                            os.makedirs('share/' + share + '/')

                    local_file = open(f, 'wb')
                    file_size, file_md5 = req_dict.get(f)
                    total_block_number = math.ceil(file_size / BLOCK_SIZE)  # calculate total block number
                    temp_dict = {f: [file_size, file_md5]}      # dictionary only contains current downloading file
                    print('downloading ' + f)
                    time_start = time.time()
                    for block_index in tqdm(range(total_block_number)):     ### main downloading part
                        rq_packet = make_package_info(FILE, temp_dict, block_index)
                        c_socket.sendall(rq_packet)

                        block_length = c_socket.recv(4)
                        block_length, = struct.unpack("!I", block_length)
                        buf = b''
                        while len(buf) < block_length:
                            block = c_socket.recv(BUFFER_SIZE)
                            buf += block
                        local_file.write(buf)
                        log.write(f + ' ' + str(block_index) + '\n')
                    local_file.close()

                    while os.path.getsize(f) != file_size:  # if error occurs re-download
                        print('re-download' + f)
                        unlock = c_socket.recv(BUFFER_SIZE)
                        local_file = open(f, 'wb')
                        for block_index in tqdm(range(total_block_number)):
                            rq_packet = make_package_info(FILE, temp_dict,block_index)
                            c_socket.sendall(rq_packet)

                            buf = b''
                            while len(buf) < BLOCK_SIZE:
                                block = c_socket.recv(BUFFER_SIZE)
                                buf += block
                            local_file.write(buf)
                            log.write(f + ' ' + str(block_index) + '\n')
                        local_file.close()

                    time_end =time.time()
                    print(f + ' totally received with time: ', round(time_end - time_start, 4))
                    share_dict.update(temp_dict)
                    print(share_dict)
                    log.close()

                if(os.path.exists('log/ticket ' + server_name + '.txt')):
                    os.remove('log/ticket ' + server_name + '.txt')
                if os.path.exists('log/log ' + server_name + '.txt'):
                    os.remove('log/log ' + server_name + '.txt')

            # elif os.path.exists('log/modify ' + server_name + '.txt'):
            #     req_dict = read_file('log/modify ' + server_name + '.txt')
            #     req_dict = json.loads(req_dict)
            #     for f in req_dict:
            #         local_file = open(f,'rb+')
            #         rq_packet = make_package_info(FILE, req_dict, 0)
            #         c_socket.sendall(rq_packet)
            #
            #         buf = b''
            #         while len(buf) < BLOCK_SIZE:
            #             block = c_socket.recv(BUFFER_SIZE)
            #             buf += block
            #         local_file.write(buf)
            #         local_file.close()
            #         print(f + 'updated')
            #     share_dict.update(req_dict)
            #     if(os.path.exists('log/modify ' + server_name + '.txt')):
            #         os.remove('log/modify ' + server_name + '.txt')
            else:   # no ticket currently
                # print('no ticket')
                time.sleep(1)

        except:
            print('old downloader connection closed')
            break


################## TCP LISTENER ##################
def listener():
    server_socket = socket(AF_INET, SOCK_STREAM)
    server_socket.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
    server_socket.bind(('', PORT))
    server_socket.listen(20)
    print('server start')
    while True:
        c_socket, addr = server_socket.accept()
        print('connected from ' + addr[0])
        th_newrecv = threading.Thread(target=sub_server, args=(c_socket, addr))     # create sub-thread for connection
        th_newrecv.daemon = True
        th_newrecv.start()


def sub_server(c_socket, addr):     # sub listener for the connection
    global early_ticket, end_addr
    while True:
        try:
            size_info = c_socket.recv(4)
            print('sth received\n')
            header_length, = struct.unpack("!I", size_info)     # length of header
            message_type = c_socket.recv(4)
            message_type, = struct.unpack("!I", message_type)   # message type

            server_recv_dict = {}
            if message_type == HELLO or message_type == NEWS:   # message type that received a dictionary
                buf = b''
                while len(buf) < header_length - 4:
                    header= c_socket.recv(header_length - 4)
                    buf += header
                server_recv_dict = json.loads(buf.decode())
                local_dict = scan_file(FILE_DIR)
                diff = compare_file(local_dict, server_recv_dict)       # comparing received and local dictionary

                if early_ticket != diff:    # eliminate redundant ticket
                    early_ticket = diff
                else:
                    print('ticket already created')
                    diff = {}

                if diff:    # if difference, make the ticket
                    ticket = json.dumps(diff)
                    print(ticket)
                    mk_file('ticket ' + addr[0] + '.txt', ticket)
                    print('new file ticket created')
                else:
                    print('no new file')

                if message_type == HELLO:
                    print('----------------------------------------------')
                else:
                    print('$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$')

            if message_type == FILE:    # message type that required to send file blocks
                block_index = c_socket.recv(4)      # current block index
                block_index, = struct.unpack("!I", block_index)
                buf = b''
                while len(buf) < header_length - 8:
                    header= c_socket.recv(header_length - 8)
                    buf += header
                server_recv_dict = json.loads(buf.decode())
                send_file, = tuple(server_recv_dict)
                send_block = get_file_block(send_file, block_index)
                c_socket.sendall(send_block)
                print('block sent-' + str(block_index))

            # if message_type == MODIFY:            # partly modified
            #     buf = b''
            #     while len(buf) < header_length - 4:
            #         header= c_socket.recv(header_length - 4)
            #         buf += header
            #     server_recv_dict = json.loads(buf.decode())
            #     local_dict = scan_file(FILE_DIR)
            #     diff = compare_file(local_dict, server_recv_dict)
            #
            #     if diff:
            #         ticket = json.dumps(diff)
            #         print(ticket)
            #         mk_file('modify ' + addr[0] + '.txt', ticket)
            #         print('modify file ticket created')
            #     else:
            #         print('no modify file')

        except:
            print(addr[0] + ' closed')
            end_addr = addr[0]
            break


##################  MAIN PART ##################
def restart():  # function to restart closed client session
    global end_addr
    while True:
        if end_addr == 'no':
            time.sleep(1)
        else:
            time.sleep(5)
            client_socket = socket(AF_INET, SOCK_STREAM)
            share_dict = scan_file(FILE_DIR)
            say_hello(end_addr, client_socket, share_dict)
            th_rereq = Process(target=downloader, args=(client_socket, IP_ADDRESS1, share_dict,))
            th_redet = Process(target=detect_send_change, args=(client_socket, share_dict,))
            th_rereq.start()
            th_redet.start()
            end_addr = 'no'
            break


def say_hello(server_name, client_socket, share_dict):     # try to establish connection and send hello message
    while True:
        try:
            client_socket.connect((server_name, PORT))
        except:
            #print("server doesn't start")
            time.sleep(1)
        else:
            print("client start")
            hello_pack = make_package_info(HELLO, share_dict)   # make pack_hello
            client_socket.sendall(hello_pack)
            print('hello send')
            break


def _argparse():    # reading parameters from commend line
    global IP_ADDRESS1
    global IP_ADDRESS2
    parser = argparse.ArgumentParser(description="input ip address")
    parser.add_argument('--ip', action='store', required=True, dest='ip', help='ips')
    IP_ADDRESS = parser.parse_args().ip.split(",")
    IP_ADDRESS1 = IP_ADDRESS[0]
    IP_ADDRESS2 = IP_ADDRESS[1]


def init():     # initial function to create dir if not exist
    if not os.path.exists(FILE_DIR):
        os.mkdir(FILE_DIR)
    elif len(os.listdir(FILE_DIR)) != 0:
        for f in os.listdir(FILE_DIR):
            if isfile('share/' + f):
                if os.path.getsize('share/' + f) > 20 * 1024 * 1024:
                    os.remove('share/' + f)
    if not os.path.exists('log/'):
        os.mkdir('log/')


def main():
    _argparse()
    init()
    share_dict = mp.Manager().dict(scan_file(FILE_DIR))     # sharing dictionary

    th_server = threading.Thread(target=listener)       # TCP listener
    th_server.start()

    client_socket1 = socket(AF_INET, SOCK_STREAM)
    client_socket2 = socket(AF_INET, SOCK_STREAM)
    th_client1 = threading.Thread(target=say_hello, args=(IP_ADDRESS1, client_socket1, share_dict))
    th_client2 = threading.Thread(target=say_hello, args=(IP_ADDRESS2, client_socket2, share_dict))
    th_client1.start()
    th_client2.start()

    th_req1 = Process(target=downloader, args=(client_socket1, IP_ADDRESS1, share_dict,))
    th_det1 = Process(target=detect_send_change, args=(client_socket1, share_dict,))
    th_req2 = Process(target=downloader, args=(client_socket2, IP_ADDRESS2, share_dict,))
    th_det2 = Process(target=detect_send_change, args=(client_socket2, share_dict,))
    th_req1.start()
    th_det1.start()
    th_req2.start()
    th_det2.start()

    restart()


if __name__ == '__main__':
    main()
