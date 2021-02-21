import socket
import time
import pypyodbc  # 导入数据库操作PPODBC
import string
import os


def db_connect():
    db_str = 'Driver={Microsoft Access Driver (*.mdb)};DBQ=E:\\online_transaction2.0\\TransDB.mdb'
    conn = pypyodbc.win_connect_mdb(db_str)  # 数据库连接
    cur = conn.cursor()
    cur.execute(
        "SELECT tran_nm,ps_idx,ps_data FROM tbl_trans_ps_fix where tran_nm between '4001' and '4601' order by tran_nm,ps_idx")  # SQL查询3001 3411 4001 4601
    num = 0
    dbresult = []

    for row in cur.fetchall():  # 遍历循环结果
        dbresult.append(row)
        num += 1
    #print(u'本次共发送交易:', num)

    conn.commit()
    cur.close()
    conn.close()
    return dbresult


def HexStrToInt(TranNo, TranId, TranData):  # 16进制转换成ACII码
    len_str = 0
    hex_code_10 = []
    Request_Message_List = []
    while len_str < len(TranData):
        hex_code_10.append(chr(int(TranData[len_str:len_str + 2], 16)))  # 将数据库中取出来的报文字段从16进制转换成ASCII码
        len_str = len_str + 2
    TranData = "".join(hex_code_10)
    TranNo = str(TranNo)
    TranId = str(TranId)  # 将数据库中取出的交易号和案例号由int型和unicode型转换成str
    Request_Message_List.append((TranNo, TranId, TranData))  # 将转换后的交易号，案例号，报文添加到列表
    return Request_Message_List


def Make_Request_Message():
    dbresult = db_connect()  # 调用db_connect方法获取表中数据，结果是一个二维列表。
    All_message_list = []
    for i in range(len(dbresult)):
        TempList = HexStrToInt(dbresult[i][0], dbresult[i][1],
                               dbresult[i][2])  # 将列表中每条数据中的交易号，案例号，交易报文传入HexStrToInt方法中做转换
        All_message_list += TempList  # 把每个转换后的报文表添加到一个总报文表。
    return All_message_list


# 开始连接通讯并发送报文
def Start_Connection(recv_ip,recv_post,send_ip,send_post):
    Send_Message_List = []
    sock_recv_addr = ((recv_ip, recv_post))
    sock_send_addr = ((send_ip, send_post))

    sockrecv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sockrecv.bind(sock_recv_addr)
    print("bind ok")
    sockrecv.listen(5)
    conn, addr = sockrecv.accept()
    print ("conn from :", addr)

    socksend = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    socksend.connect(sock_send_addr)
    print ("connect ok!")
    time.sleep(3)

    Send_Message_List = Make_Request_Message()
    formatTime = time.localtime()
    filename = time.strftime("%Y%m%d-%H%M%S",  formatTime)
    pathname = "c:\\"+filename
    print ("pathname=",pathname)
    if os.path.exists(filename) and os.path.isdir(filename):
        pass
    else:
        os.chdir("c:\\")
        os.mkdir(filename)
    fp = open(pathname+ "\\log.txt", "w")
    fp2 = open(pathname + "\\errlog.txt", "w")
    countnum = 0
    for i in range(len(Send_Message_List)):
        try:
            ret = socksend.sendall(Send_Message_List[i][2])
            TranNo = Send_Message_List[i][0]
            TranId = Send_Message_List[i][1]
            conn.settimeout(5)
            # time.sleep(2)
            while 1:
                responseLen = conn.recv(4)
                print("responseLen1=", responseLen)
                if responseLen != "0000":
                    break
            print ("responseLen2=", responseLen)
            response_data = conn.recv(int(responseLen))
            print ("response_data=", response_data)
            #print u"请求交易为：", TranNo
            #print u"响应交易为：", response_data[:4]
            if response_data[4:10] == "000000" and response_data[:4] == TranNo:
                countnum += 1
                fp.write(TranNo + "-" + TranId + " " * 4 + response_data[4:10] + "\n")
            elif response_data[4:10] == "000000" and response_data[:4] != TranNo:
                fp2.write(TranNo + "-" + TranId + " " * 4 + "Transaction dislocation" + "\n")
            elif responseLen == "0000":
                fp2.write(TranNo + "-" + TranId + " " * 4 + "No Response Message Received" + "\n")
            else:
                fp2.write(TranNo + "-" + TranId + " " * 4 + response_data[4:10] + "unknow error" + "\n")
        except socket.timeout as e:
            print(e)
            fp2.write(TranNo + "-" + TranId + " " * 4 + "TIMEOUT" + "\n")
        time.sleep(2)

    print("countnum====", countnum)
    sockrecv.close()
    conn.close()
    socksend.close()
    fp.close()
    fp2.close()

Start_Connection('10.192.00.01',12345,'10.192.00.02',67890) #UMS
