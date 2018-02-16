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
using namespace std;
using namespace rapidjson;

Document document;
unsigned long long reqID=0;

void
error(const char *msg){
    perror(msg);
    exit(1);
}


string prepareREGISTERmessageinjson(string hostid,int port)
{
    string str=" { \"reqid\" :"+ to_string(reqID) +", \"reqtype\" : \"registerclientreq\", \"clienthostid\" : \""+hostid+"\", \"clientport\" : "+to_string(port)+" } ";
    reqID++;
    return str;
}

string preparePUTmessageinjson(string key,string value)
{
    string str=" { \"reqid\" :"+ to_string(reqID) +", \"reqtype\" : \"putreq\", \"key\" : \""+key+"\", \"value\" : \""+value+"\" } ";
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
    reqID++;
    return str;
}



string prepareGETmessageinjson(string key)
{
    string str=" { \"reqid\" :"+ to_string(reqID) +", \"reqtype\" : \"getreq\", \"key\" : \""+key+"\" } ";
    reqID++;
    return str;
}

int main(int argc,char *argv[]){

    int sock;
    if(argc!=5)
        error("Not Enough Arguments!!");
    struct sockaddr_in server;
    struct hostent *serverhost;
    char *serverPort = argv[2];
    char *serverHost = argv[1];
    char *clientPort = argv[4];
    char *clientIp = argv[3];
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
    //char buffer[200];
    //sprintf(buffer,"%s#%s#%s","0",clientIp,clientPort);
    string str(clientIp);
    string buff=prepareREGISTERmessageinjson(str,atoi(clientPort));
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
        error("Error connect");

    while(1)
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
            char valuereceived[256];
            recv(sock3,valuereceived,256,0);
            cout<<"Value is"<<valuereceived<<endl;

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

    }

    }

    return 0;
}
