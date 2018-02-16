#include<stdio.h>
#include<bits/stdc++.h>
#include<sys/types.h>
#include<sys/socket.h>
#include<netinet/in.h>
#include<string.h>
#include<stdlib.h>
#include<sys/stat.h>
#include<sys/sendfile.h>
#include<fcntl.h>
#include<netdb.h>
#include<unistd.h>
#include "rapidjson/document.h"
#include "rapidjson/writer.h"
#include "rapidjson/stringbuffer.h"
#include <arpa/inet.h>
using namespace std;
using namespace rapidjson;


#define BEATPORT 15200
#define LISPORT 51000
#define OFFSET 3000

Document document;
unsigned long long reqID=0;

typedef struct{
    char serverIpAddress[100];
    int serverPort;
}Udpparam;


typedef struct {
    int uselessport;
    string preIP;
    string prePort;
    string succIp;
    string succPort;
}uselessparam;

map<string,string> MyKeyValueMap;
map<string,string> SuccessorKeyValueMap;

void
error(const char *msg){
    perror(msg);
    exit(1);
}


string prepareREGISTERmessageinjson(string hostid,int port)
{
    string str=" { \"reqid\" :"+ to_string(reqID) +", \"reqtype\" : \"registerslavereq\", \"slavehostid\" : \""+hostid+"\", \"slaveport\" : "+to_string(port)+" } ";
    reqID++;
    return str;
}


string preparePREPAREACKSLAVEmessageinjson(int id)//pvalue=0 failed registration
{
    string str=" { \"reqid\" :"+ to_string(reqID) +", \"reqtype\" : \"prepareackpc\", \"prepared\" : "+to_string(id)+" } ";
    reqID++;
    return str;
}


string prepareCOMMITACKSLAVEmessageinjson(int id)//pvalue=0 failed registration
{
    string str=" { \"reqid\" :"+ to_string(reqID) +", \"reqtype\" : \"commitackpc\", \"committed\" : "+to_string(id)+" } ";
    reqID++;
    return str;
}

string preparePUTmessageinjson(string key,string value)
{
    string str=" { \"reqid\" :"+ to_string(reqID) +", \"reqtype\" : \"putreq\", \"key\" : \""+key+"\", \"value\" : \""+value+"\" } ";
    reqID++;
    return str;
}

string prepareGETACKmessageinjson(string key,string value)
{
    string str=" { \"reqid\" :"+ to_string(reqID) +", \"reqtype\" : \"getack\", \"key\" : \""+key+"\", \"value\" : \""+value+"\" } ";
    reqID++;
    return str;
}


string prepareDELmessageinjson(string key)
{
    string str=" { \"reqid\" :"+ to_string(reqID) +", \"reqtype\" : \"delreq\", \"key\" : \""+key+"\" } ";
    reqID++;
    return str;
}


string prepareBYEmessageinjson()
{
    string str=" { \"reqid\" :"+ to_string(reqID) +", \"reqtype\" : \"byereq\" } ";
    cout<<"Byee...!"<<endl;
    reqID++;
    return str;
}



string prepareGETmessageinjson(string key)
{
    string str=" { \"reqid\" :"+ to_string(reqID) +", \"reqtype\" : \"getreq\", \"key\" : \""+key+"\" } ";
    reqID++;
    return str;
}


void*
heartbeat(void *param){
    char serverip[100];
    Udpparam node=*((Udpparam *)param);
    strcpy(serverip,node.serverIpAddress);
    int serverport=node.serverPort;
    struct sockaddr_in address;
    struct sockaddr_in serv_addr;
    int sock;
    char heartbeatMessage[256];
    strcpy(heartbeatMessage,to_string(document["registered"].GetInt()).c_str());
    while(1){
        if((sock=socket(AF_INET, SOCK_DGRAM, 0))<0){
            printf("\n Socket Creation Error\n");
        }
        memset(&serv_addr, '0', sizeof(serv_addr));
      /*  serv_addr.sin_family = AF_INET;
        serv_addr.sin_port=serverport;*/
        serv_addr.sin_family=AF_INET;
    serv_addr.sin_port=htons(serverport);
  //  server.sin_addr.s_addr=INADDR_ANY;

        if(inet_pton(AF_INET, serverip, &serv_addr.sin_addr)<=0){
                printf("\nInvalid address/ Address not supported \n");
        }
        cout<<serverport<<serverip<<endl;
        if (connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0){
                printf("\nConnection Failed \n");
        }
        send(sock , heartbeatMessage , strlen(heartbeatMessage) , 0 );
        cout<<"sent"<<heartbeatMessage<<endl;
        sleep(5);
    }
}


void
init(string str,string str1){

    FILE *fp,*fp1;

    fp=fopen(str.c_str(),"r");
    fp1=fopen(str1.c_str(),"r");

    char temp[500],temp1[500];
    while(fgets(temp,500,fp)!=NULL){

        char *key=new char[200];
        char *value=new char[200];
        sscanf(temp,"%s#%s",key,value);
        string keys(key);
        string values(value);
        MyKeyValueMap[keys]=values;
    }

        while(fgets(temp1,500,fp)!=NULL){

        char *key=new char[200];
        char *value=new char[200];
        sscanf(temp1,"%s#%s",key,value);
        string keys(key);
        string values(value);
        SuccessorKeyValueMap[keys]=values;

    }

}

void *
backuplistener(void *currentport){
    struct sockaddr_in server, client;
    int sock1,sock2;
    uselessparam attr1;
    attr1=*((uselessparam *)currentport);

    int port1=attr1.uselessport;
    string predIp=attr1.preIP;
    string predPort=attr1.prePort;
    string succIp=attr1.succIp;
    string succPort=attr1.succPort;





    sock1=socket(AF_INET, SOCK_STREAM, 0);
    if(sock1<0)
        error("Error Creating Socket");
    bzero((char *)&server, sizeof(server));
    server.sin_family=AF_INET;
    server.sin_port=htons(port1+OFFSET);
    cout<<"Portandoffset "<<port1+OFFSET<<endl;
    server.sin_addr.s_addr=INADDR_ANY;
    if(bind(sock1, (struct sockaddr*)&server, sizeof(server))<0)
        error("Error Binding");
    if(listen(sock1, 1)<0)
        error("Error Listen");
    socklen_t clientLen = sizeof(client);
    while((sock2=accept(sock1, (struct sockaddr*)&client,&clientLen))>0){

        //if((strcmp(inet_ntoa(client.sin_addr),predIp)==0) && (int) ntohs(client_addr.sin_port)==)
        {


        string successorData="";
        for(auto it=SuccessorKeyValueMap.begin();it!=SuccessorKeyValueMap.end();it++){
            successorData+=(*it).first;
            successorData+=":";
            successorData+=(*it).second;
            successorData+="#";
        }
        send(sock2,successorData.c_str(),1000,0);
        }
    }
}





int main(int argc,char *argv[]){

    int sock;
    FILE *fp,*fp1;


    if(argc!=5)
        error("Not Enough Arguments!!");
    struct sockaddr_in server;
    struct hostent *serverhost;
    char *serverPort = argv[2];
    char *serverHost = argv[1];
    char *slavePort = argv[4];
    char *slaveIp = argv[3];
    sock=socket(AF_INET, SOCK_STREAM, 0);
    if(sock<0)
        error("Error Creating Socket");
    server.sin_family=AF_INET;
    server.sin_port=htons(atoi(serverPort));
    server.sin_addr.s_addr=0;
    serverhost=gethostbyname(serverHost);
    bcopy((char *)serverhost->h_addr,(char *)&server.sin_addr.s_addr,serverhost->h_length);
    if (connect(sock,(struct sockaddr*)&server,sizeof(server)) < 0)
        error("Error connect");
        string slaveip(slaveIp);
        string str2(slaveip+"_"+slavePort);
        string str1(slaveip+"_"+slavePort+"_successor");
        fp=fopen(str2.c_str(),"a");
        fp1=fopen(str1.c_str(),"a");


    //char buffer[200];
    //sprintf(buffer,"%s#%s#%s","0",clientIp,clientPort);
    init(str2,str1);
    string str(slaveIp);
    string buff=prepareREGISTERmessageinjson(str,atoi(slavePort));
    cout<<buff<<endl;
    send(sock,buff.c_str(),200,0);

    char ack[300];
    recv(sock, ack, 300, 0);
    string ackstring(ack);
    cout<<ackstring<<endl;




    if (document.ParseInsitu(ack).HasParseError())
    {
        cout<<"Error Parsing Document"<<endl;
        return 0;
        //pvalue=0;
    }
       // return;
    else{
    assert(document.IsObject());

    assert(document.HasMember("registered"));
    assert(document["registered"].IsInt());


    printf("\nParsing to document succeeded.\n");

    assert(document["port"].IsInt());

    //pvalue=1;

    cout<<"This is registerid:"<<document["registered"].GetInt()<<endl;
    if(document["registered"].GetInt()>=1)
         cout<<"Registration Successful Got userid:"<<document["registered"].GetInt()<<endl;
    else cout<<"Registration Unsuccessful"<<endl;


    Udpparam param;
    strcpy(param.serverIpAddress,serverHost);
    param.serverPort=BEATPORT;
    pthread_t threadid;
    if(pthread_create(&threadid,NULL,heartbeat,(void*)&param)<0){
      error("Thread error");
    }






    //document["port"].GetInt()


    int sock3;
    struct sockaddr_in server;
    struct hostent *serverhost;
    //char *serverPort = argv[2];
    //char *serverHost = argv[1];
    //char *clientPort = argv[4];
    //char *clientIp = argv[3];
    sock3=socket(AF_INET, SOCK_STREAM, 0);
    cout<<"Port"<<document["port"].GetInt()<<endl;
    if(sock3<0)
        error("Error Creating Socket");
    server.sin_family=AF_INET;
    server.sin_port=htons(document["port"].GetInt());
    server.sin_addr.s_addr=0;
    serverhost=gethostbyname(serverHost);
    bcopy((char *)serverhost->h_addr,(char *)&server.sin_addr.s_addr,serverhost->h_length);
    if (connect(sock3,(struct sockaddr*)&server,sizeof(server)) < 0)
        error("Error connect2");


    cout<<"I got port :"<<document["port"].GetInt()<<endl;


    char receivers[100];
    recv(sock3,receivers,100,0);
    string receiverString(receivers);
    string predIp=receiverString.substr(0,receiverString.find(":"));
    string predPort=receiverString.substr(receiverString.find(":")+1,receiverString.find("#"));

    string Ipsuc=receiverString.substr(receiverString.find("#")+1);

    string succIP="";
    string succPort="";

    succIP=Ipsuc.substr(0,Ipsuc.find(":"));
    succPort=Ipsuc.substr(Ipsuc.find(":")+1);

    cout<<predIp<<endl;
    cout<<succIP<<endl;
    cout<<predPort<<endl;
    cout<<succPort<<endl;




    uselessparam attr;
    attr.uselessport=atoi(slavePort);
    attr.preIP=predIp;
    attr.prePort=predPort;
    attr.succIp=succIP;
    attr.succPort=succPort;

    pthread_t listenerthreadid;
    //int useless=document["port"].GetInt();
    if(pthread_create(&listenerthreadid,NULL,backuplistener,(void*)&attr)<0){
      error("Thread error");
    }


    sleep(5);

    int sock5;
    struct sockaddr_in server1;
    struct hostent *serverhost1;
    //char *serverPort = argv[2];
    //char *serverHost = argv[1];
    //char *clientPort = argv[4];
    //char *clientIp = argv[3];
    sock5=socket(AF_INET, SOCK_STREAM, 0);
    cout<<"Port"<<atoi(succPort.c_str())+OFFSET<<endl;
    if(sock5<0)
        error("Error Creating Socket");
    server1.sin_family=AF_INET;
    server1.sin_port=htons(atoi(succPort.c_str())+OFFSET);
    server1.sin_addr.s_addr=0;
    serverhost1=gethostbyname(succIP.c_str());
    bcopy((char *)serverhost1->h_addr,(char *)&server1.sin_addr.s_addr,serverhost1->h_length);

    if (connect(sock5,(struct sockaddr*)&server1,sizeof(server1)) < 0)
        error("Error connect3");
    else
    {
        char mydata[1000];
        recv(sock5,mydata,1000,0);
        string mydatastring(mydata);
        while(!mydatastring.empty())
        {
            string key="",value="";
            key=mydatastring.substr(0,mydatastring.find(':'));
            mydatastring=mydatastring.substr(mydatastring.find(':')+1);
            value=mydatastring.substr(0,mydatastring.find('#'));
            MyKeyValueMap[key]=value;
            if(mydatastring.size()!=1)
                mydatastring=mydatastring.substr(mydatastring.find('#')+1);
            else
                mydatastring="";
        }
    }



    while(1)
    {
        char masterCommand[300];
        recv(sock3,masterCommand,300,0);
        cout<<masterCommand<<endl;
        string masterCommandString(masterCommand);
        if (document.ParseInsitu(masterCommand).HasParseError())
        {
            cout<<"DocumentParsing error"<<endl;
        }
        else
        {
            cout<<"Parsing successful"<<endl;
            cout<<masterCommandString<<endl;
            if(strcmp(document["reqtype"].GetString(),"getreq")==0)
            {
                string responseValue(document["key"].GetString());
                if(MyKeyValueMap.find(responseValue)!=MyKeyValueMap.end())
                {


                cout<<MyKeyValueMap[responseValue]<<endl;
                string corval=MyKeyValueMap[responseValue];

                send(sock3,(prepareGETACKmessageinjson(responseValue,corval)).c_str(),300,0);
                }
                else if (SuccessorKeyValueMap.find(responseValue)!=SuccessorKeyValueMap.end())
                {


                cout<<SuccessorKeyValueMap[responseValue]<<endl;
                string corval=SuccessorKeyValueMap[responseValue];

                send(sock3,(prepareGETACKmessageinjson(responseValue,corval)).c_str(),300,0);
                }
                else{
                    send(sock3,"NOT WORKING",300,0);
                }

            }

            else if(strcmp(document["reqtype"].GetString(),"prepareput")==0)
            {

                cout<<document["reqtype"].GetString()<<endl;

                int successor=document["prepare"].GetInt();
                cout<<successor<<endl;
                string getmessageinjson1 = preparePREPAREACKSLAVEmessageinjson(1);
                            send(sock3,getmessageinjson1.c_str(),300,0);

                            char receiveResponseCommand[300];
                            recv(sock3,receiveResponseCommand,300,0);
                cout<<receiveResponseCommand<<endl;
                        string key(document["key"].GetString());
                        string corrvalue(document["value"].GetString());

                        if (document.ParseInsitu(receiveResponseCommand).HasParseError())
                        {
                            cout<<"Parse Error"<<endl;
                        }
                        else
                        {
                            if(document["commit"].GetInt()==1)
                            {
                                    if(successor==0)
                                    {
                                        MyKeyValueMap[key]=corrvalue;
                                        string s1(key+"#"+corrvalue);
                                        fprintf(fp,"%s\n",s1.c_str());
                                        cout<<"MAIN "<<key<<" "<<corrvalue<<endl;
                                    }
                                    else{
                                        SuccessorKeyValueMap[key]=corrvalue;
                                         string s2(key+"#"+corrvalue);
                                        fprintf(fp1,"%s\n",s2.c_str());
                                        cout<<"SUCCESSOR "<<key<<" "<<corrvalue<<endl;
                                    }

                            }
                            else
                            {


                            }
                        }
            }


            else if(strcmp(document["reqtype"].GetString(),"preparedel")==0)
            {

                //int successor=document["prepare"].GetInt();
                //cout<<document["reqtype"].GetString()<<endl;

                //int successor=document["prepare"].GetInt();
                //cout<<successor<<endl;
                string getmessageinjson1 = preparePREPAREACKSLAVEmessageinjson(1);
                send(sock3,getmessageinjson1.c_str(),300,0);

                char receiveResponseCommand[300];
                recv(sock3,receiveResponseCommand,300,0);
                cout<<receiveResponseCommand<<endl;
                string key(document["key"].GetString());
                //string corrvalue(document["value"].GetString());

                        if (document.ParseInsitu(receiveResponseCommand).HasParseError())
                        {
                            cout<<"Parse Error"<<endl;
                        }
                        else
                        {
                            if(document["commit"].GetInt()==1)
                            {
                                    if(MyKeyValueMap.find(key)!=MyKeyValueMap.end())
                                            MyKeyValueMap.erase(key);
                                    else    SuccessorKeyValueMap.erase(key);
                            }
                            else
                            {


                            }
                        }
                /*string getmessageinjson1 = preparePREPAREACKSLAVEmessageinjson(1);
                send(sock3,getmessageinjson1.c_str(),300,0);

                char receiveResponseCommand[300];
                recv(sock3,receiveResponseCommand,300,0);

                if(document["commit"].GetInt()==1)
                {
                        string key(document["key"].GetString());
                        string corrvalue(document["value"].GetString());
                        //if(successor==0)
                        if(MyKeyValueMap.find(key)!=MyKeyValueMap.end())
                            MyKeyValueMap.erase(key);
                        if(SuccessorKeyValueMap.find(key)!=SuccessorKeyValueMap.end())
                            SuccessorKeyValueMap.erase(key);

                }
                else
                {


                }*/
            }


        }

    }

    /*while(1)
    {
        cout<<"Select A Request for server:"<<endl;
        cout<<"1.GET REQUEST"<<endl;
        cout<<"2.PUT REQUEST"<<endl;
        cout<<"3.DEL REQUEST"<<endl;
        cout<<"4.End Connection"<<endl;
        int selection;
        cin>>selection;
        if(selection==1)
        {
            cout<<"Please enter a Key to find : ";
            string key;
            cin>>key;
            string getmessageinjson = prepareGETmessageinjson(key);
            cout<<getmessageinjson;
            //jsonstringtodocument(getmessageinjson);
            send(sock3,getmessageinjson.c_str(),100,0);
            cout<<"Sent Successfully"<<endl;
        }
        else if(selection==2)
        {
            cout<<"Please enter Key : ";
            string key;
            cin>>key;
            cout<<"Please enter Value : ";
            string value;
            cin>>value;

            string getmessageinjson = preparePUTmessageinjson(key,value);
            cout<<getmessageinjson;
            //jsonstringtodocument(getmessageinjson);
            send(sock3,getmessageinjson.c_str(),100,0);
            cout<<"Sent Successfully"<<endl;
        }
        else if(selection==3)
        {
            cout<<"Please enter Key to delete : ";
            string key;
            cin>>key;
            string getmessageinjson = prepareDELmessageinjson(key);
            cout<<getmessageinjson;
            //jsonstringtodocument(getmessageinjson);
            send(sock3,getmessageinjson.c_str(),100,0);
            cout<<"Sent Successfully"<<endl;
        }
        else
        {
            string getmessageinjson = prepareBYEmessageinjson();
            cout<<getmessageinjson;
            send(sock3,getmessageinjson.c_str(),100,0);
            close(sock3);
            break;
        }

    }*/

    }

    return 0;
}

