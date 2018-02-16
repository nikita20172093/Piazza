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
#include<unistd.h>
#include<sys/time.h>
#include<pthread.h>
#include<libgen.h>
#include<arpa/inet.h>
#include "rapidjson/document.h"
#include "rapidjson/writer.h"
#include "rapidjson/stringbuffer.h"


using namespace rapidjson;
using namespace std;

unsigned long long reqID=0;
unsigned long long currentclientuid=1;
unsigned long long currentslaveuid=1;
int numberOfSlaveServers;
#define CACHE_SIZE 50
#define HEARTBEATPORT 15200

Document document;

int globalclientport=18933;
int globalslaveport=13210;

string cacheArray[CACHE_SIZE];

int currentIndex=0;
int isFull=CACHE_SIZE;


map <string,int> cacheMap;
map <string,string> cacheKeyValueMapping;
map <int,string> slaveUidToipport;
map <int,string> clientUidToipport;
map <int, int> uidToSocket;
map <int,int> timedout;
map <int,bool> isLive;
map <string,int> ipportToUid;
//map <int,bool> isUidUp;



typedef struct{
  int slavePort;
  int slaveUid;
}parameter;

void
error(const char *msg){
    perror(msg);
    exit(1);
}

string prepareREGISTERACKCLIENTmessageinjson(int pvalue,int port)//pvalue=0 failed registration
{
    string str=" { \"reqid\" :"+ to_string(reqID) +", \"reqtype\" : \"acknowledgeclientreq\", \"registered\" : "+to_string(pvalue)+", \"port\" : "+to_string(port)+" } ";
    reqID++;
    return str;
}

string prepareREGISTERACKSLAVEmessageinjson(int qvalue,int port)//pvalue=0 failed registration
{
    string str=" { \"reqid\" :"+ to_string(reqID) +", \"reqtype\" : \"acknowledgeslavereq\", \"registered\" : "+to_string(qvalue)+", \"port\" : "+to_string(port)+" } ";
    reqID++;
    return str;
}


string prepareSLAVEGETmessageinjson(string key)
{
    string str=" { \"reqid\" :"+ to_string(reqID) +", \"reqtype\" : \"getreq\", \"key\" : \""+key+"\" } ";
    reqID++;
    return str;
}


string prepareCOMMITSLAVEmessageinjson(int id)
{
    string str=" { \"reqid\" :"+ to_string(reqID) +", \"reqtype\" : \"commitpc\", \"commit\" :"+to_string(id)+" } ";
    reqID++;
    return str;
}

string preparePREPARESLAVEPUTmessageinjson(string key,string value,int id)//pvalue=0 failed registration
{
    string str=" { \"reqid\" :"+ to_string(reqID) +", \"reqtype\" : \"prepareput\", \"prepare\" : "+to_string(id)+", \"key\" : \""+key+"\", \"value\" : \""+value+"\", \"successor\" : "+to_string(id)+" } ";
    reqID++;
    return str;
}


string preparePREPARESLAVEDELmessageinjson(string key)//pvalue=0 failed registration
{
    string str=" { \"reqid\" :"+ to_string(reqID) +", \"reqtype\" : \"commitdel\", \"key\" : \""+key+"\" } ";
    reqID++;
    return str;
}
//CACHING STARTS

void newRequest(string ele){


map<string,int>::iterator it;


it=cacheMap.find(ele);

if(it!=cacheMap.end()){

//cout<<"HII";
cacheMap.find(ele)->second=1;

cout<<"\n HIT: "<<cacheMap.find(ele)->second<<"\n";
//return true;
}
else
{
        if(isFull>0){
                cacheArray[currentIndex]=ele;
                currentIndex=(currentIndex%CACHE_SIZE)+1;
                cacheMap.insert(pair<string,int>(ele,0));
                 isFull=isFull-1;
                  cout<<currentIndex<<" "<<isFull<<endl;


        }
        else{

                it=cacheMap.find(cacheArray[(currentIndex)%CACHE_SIZE]);
                while(it->second==1){

		it->second=0;
                currentIndex=((currentIndex+1)%CACHE_SIZE);
                it=cacheMap.find(cacheArray[currentIndex]);

                }

                if(it!=cacheMap.end())
                   cacheMap.erase(it);

               // cacheKeyValueMapping.erase(cacheKeyValueMapping.find(it->first));
                cacheMap.insert(pair<string,int>(ele,0));
                cacheArray[(currentIndex)%CACHE_SIZE]=ele;
                currentIndex=(currentIndex+1)%CACHE_SIZE;
        }
      //  cacheKeyValueMapping[ele]=value;

}
//return false;
}

//CACHING ENDS



void jsonstringtodocument(string jsonstring)
{


    cout<<jsonstring<<endl;
#if 0
    // "normal" parsing, decode strings to new buffers. Can use other input stream via ParseStream().
    if (document.Parse(json).HasParseError())
        return 1;
#else
    // In-situ parsing, decode strings directly in the source string. Source must be string.
    char buffer[jsonstring.length()];
    memcpy(buffer, jsonstring.c_str(), jsonstring.length());
    cout<<buffer<<endl;

    if (document.ParseInsitu(buffer).HasParseError())
        return;
#endif

    assert(document.IsObject());

    assert(document.HasMember("clienthostid"));
    assert(document["clienthostid"].IsString());


    printf("\nParsing to document succeeded.\n");


    cout<<"This is clienthostid:"<<document["clienthostid"].GetString()<<endl;


    return;
}

// made by vineet
int hashfunction(string key)
{
    int modval=key.length()%4;
    int n=key.length()-modval;
    string hashedString;
    for(int i=0;i<n;i+=4){
        int num1=(int)key[i];
        int num2=(int)key[i+1];
        int num3=(int)key[i+2];
        int num4=(int)key[i+3];
        int num=(num1+num2+num3+num4)%2;
        hashedString+=to_string(num);
    }
    int num=0;
    for(int i=n;i<key.length();i++){
        num+=key[i];
    }
    if(modval!=0){
        num=num%2;
        hashedString+=to_string(num);
    }
    return stoi(hashedString, nullptr, 2);

}

void*
connectSlave(void *node){
    int port=(*((parameter*)node)).slavePort-1;
    int slavesock=(*((parameter*)node)).slaveUid;
    cout<<"Port"<<port<<endl;
    struct sockaddr_in server, client;
    int sock1,sock2;
    sock1=socket(AF_INET, SOCK_STREAM, 0);
    if(sock1<0)
        error("Error Creating Socket");
    bzero((char *)&server, sizeof(server));
    server.sin_family=AF_INET;
    server.sin_port=htons(port);
    server.sin_addr.s_addr=INADDR_ANY;
    cout<<"Stop1"<<endl;
    if(bind(sock1, (struct sockaddr*)&server, sizeof(server))<0)
        error("Error Binding");
    if(listen(sock1, 1)<0)
        error("Error Listen");
    socklen_t clientLen = sizeof(client);
    pthread_t threadid;
    cout<<"Stop2"<<endl;
    sock2=accept(sock1,(struct sockaddr*)&client,&clientLen);
    cout<<"Stop3"<<endl;
    uidToSocket[slavesock]=sock2;
    cout<<"Connection done with slave at port "<<port<<endl;
    cout<<"Connection done with slave at port "<<slavesock<<endl;
    while(true){
        if(uidToSocket.find(numberOfSlaveServers)!=uidToSocket.end())
        {
            string message;
            int pre=(slavesock-1)%numberOfSlaveServers;
            int suc=(slavesock+1)%numberOfSlaveServers;

            if(pre==0)
                pre=numberOfSlaveServers;
            if(suc==0)
                suc=numberOfSlaveServers;


            string precessor=slaveUidToipport[pre];
            string successor=slaveUidToipport[suc];
            message=precessor+"#"+successor;
            cout<<"Mymessage"<<message<<endl;
            send(sock2,message.c_str(),100,0);
            break;
        }
    }
   /* while(true)
    {
        char queryarray[300];
        bzero(queryarray, 300);
        recv(sock2, queryarray, 300, 0);
        string querystring(queryarray);

        if (document.ParseInsitu(queryarray).HasParseError())
        {
            cout<<"Error Parsing Query"<<endl;
        }
           // return;
        else{
        assert(document.IsObject());
        assert(document.HasMember("reqtype"));
        assert(document["reqtype"].IsString());
        //cout<<"T"<<document["reqtype"].GetString()<<"X"<<endl;
        //if(document["reqtype"].GetString()=="putreq")
        if(strcmp(document["reqtype"].GetString(),"putreq")==0)
            cout<<"This is Put Request"<<endl;
        else if(strcmp(document["reqtype"].GetString(),"getreq")==0)
            cout<<"This is Get Request"<<endl;
        else if(strcmp(document["reqtype"].GetString(),"delreq")==0)
            cout<<"This is Del Request"<<endl;
        else if(strcmp(document["reqtype"].GetString(),"byereq")==0)
            {cout<<"Connection ends"<<endl;close(sock2);break;}

        }
    }*/
}

void*
connectClient(void *node){
    int port=*((int *)node)-1;
    cout<<"Port"<<port<<endl;
    struct sockaddr_in server, client;
    int sock1,sock2;
    sock1=socket(AF_INET, SOCK_STREAM, 0);
    if(sock1<0)
        error("Error Creating Socket");
    bzero((char *)&server, sizeof(server));
    server.sin_family=AF_INET;
    server.sin_port=htons(port);
    server.sin_addr.s_addr=INADDR_ANY;
    if(bind(sock1, (struct sockaddr*)&server, sizeof(server))<0)
        error("Error Binding");
    if(listen(sock1, 1)<0)
        error("Error Listen");
    socklen_t clientLen = sizeof(client);
    pthread_t threadid;
    sock2=accept(sock1, (struct sockaddr*)&client,&clientLen);
    while(true)
    {


        char queryarray[300];
        bzero(queryarray, 300);
        recv(sock2, queryarray, 300, 0);
        string querystring(queryarray);
        cout<<queryarray<<endl;
        if (document.ParseInsitu(queryarray).HasParseError())
        {
            cout<<"Error Parsing Query"<<endl;
            pthread_exit(NULL);
        }
           // return;
        else{
        assert(document.IsObject());
        assert(document.HasMember("reqtype"));
        assert(document["reqtype"].IsString());
        //cout<<"T"<<document["reqtype"].GetString()<<"X"<<endl;
        //if(document["reqtype"].GetString()=="putreq")
        if(strcmp(document["reqtype"].GetString(),"putreq")==0)
            {
                cout<<"This is Put Request"<<endl;

                string key(document["key"].GetString());
                string corrvalue(document["value"].GetString());

                int hashedkey=hashfunction(key);

                int slaveServerNumber=((hashedkey%numberOfSlaveServers)+1);

                cout<<slaveServerNumber<<endl;

                string ipPortCombination=slaveUidToipport[slaveServerNumber];

                string ipaddress=ipPortCombination.substr(0, ipPortCombination.find(":"));

                string portaddress=ipPortCombination.substr(ipPortCombination.find(":")+1);

                cout<<"IP "<<ipaddress<<" PORT "<<portaddress<<endl;
                cout<<corrvalue<<endl;
                int successor=(slaveServerNumber%numberOfSlaveServers)+1;


                string ipPortCombination1=slaveUidToipport[successor];

                string ipaddress1=ipPortCombination1.substr(0, ipPortCombination1.find(":"));

                string portaddress1=ipPortCombination1.substr(ipPortCombination1.find(":")+1);

                cout<<successor<<endl;
                int slavesock=uidToSocket[slaveServerNumber];
                int slavesocksuccessor=uidToSocket[successor];

                string getmessageinjson1 = preparePREPARESLAVEPUTmessageinjson(key,corrvalue,0);
                send(slavesock,getmessageinjson1.c_str(),300,0);

                string getmessageinjson2 = preparePREPARESLAVEPUTmessageinjson(key,corrvalue,1);
                send(slavesocksuccessor,getmessageinjson2.c_str(),300,0);

                char receiveResponse[300];


                recv(slavesock,receiveResponse,300,0);



                char receiveResponse1[300];

                recv(slavesocksuccessor,receiveResponse1,300,0);

                Document reply1,reply2;

                if (reply1.ParseInsitu(receiveResponse).HasParseError())
                {
                    cout<<"DocumentParsing error"<<endl;
                }
                else
                {
                    cout<<"Parsing successful"<<endl;

                if (reply2.ParseInsitu(receiveResponse1).HasParseError())
                {
                    cout<<"DocumentParsing error"<<endl;
                }
                else
                {
                    cout<<"Parsing successful"<<endl;
                    cout<<"R1:"<<receiveResponse1<<endl;
                    cout<<"R2:"<<receiveResponse<<endl;

                    if(reply2["prepared"].GetInt()==1 && reply1["prepared"].GetInt()==1)
                    {
                             getmessageinjson1 = prepareCOMMITSLAVEmessageinjson(1);
                            send(slavesock,getmessageinjson1.c_str(),300,0);

                             getmessageinjson2 = prepareCOMMITSLAVEmessageinjson(1);
                            send(slavesocksuccessor,getmessageinjson2.c_str(),300,0);



                    }
                    else
                    {

                            getmessageinjson1 = prepareCOMMITSLAVEmessageinjson(0);
                            send(slavesock,getmessageinjson1.c_str(),300,0);

                             getmessageinjson2 = prepareCOMMITSLAVEmessageinjson(0);
                            send(slavesocksuccessor,getmessageinjson2.c_str(),300,0);
                    }

                }

                }

                //preparePREPARESLAVEDELmessageinjson()

               // cacheKeyValueMapping[key]=corrvalue;

            }

        else if(strcmp(document["reqtype"].GetString(),"getreq")==0)
            {
                cout<<"This is Get Request"<<endl;

                string key(document["key"].GetString());

                int hashedkey=hashfunction(key);

                int slaveServerNumber=((hashedkey%numberOfSlaveServers)+1);

                cout<<slaveServerNumber<<endl;

                string ipPortCombination=slaveUidToipport[slaveServerNumber];

                string ipaddress=ipPortCombination.substr(0, ipPortCombination.find(":"));

                string portaddress=ipPortCombination.substr(ipPortCombination.find(":")+1);

                cout<<"IP "<<ipaddress<<" PORT "<<portaddress<<endl;

                if(cacheKeyValueMapping.find(key)!=cacheKeyValueMapping.end())
                {
                    send(sock2,cacheKeyValueMapping[key].c_str(),256,0);
                    cout<<"It's a hit found successfully value is "<<cacheKeyValueMapping[key]<<endl;
                }
                else{
                cout<<"It's a miss unsuccessfully!"<<endl;


                int slavesock=uidToSocket[slaveServerNumber];
                string getmessageinjson1 = prepareSLAVEGETmessageinjson(key);
                send(slavesock,getmessageinjson1.c_str(),300,0);


                char receiveResponse[300];


                recv(slavesock,receiveResponse,300,0);

                cout<<receiveResponse<<endl;
                Document reply1;

                if (reply1.ParseInsitu(receiveResponse).HasParseError())
                {
                    cout<<"DocumentParsing error"<<endl;

                    int suc=(slavesock+1)%numberOfSlaveServers;

                    if(suc==0)
                        suc=numberOfSlaveServers;

                    int slavesock1=uidToSocket[suc];
                    string getmessageinjson1 = prepareSLAVEGETmessageinjson(key);
                    send(slavesock1,getmessageinjson1.c_str(),300,0);


                    char receiveResponse[300];


                    recv(slavesock1,receiveResponse,300,0);

                    cout<<receiveResponse<<endl;
                    Document reply2;


                    if (reply2.ParseInsitu(receiveResponse).HasParseError())
                    {
                        cout<<"NOT FOUND"<<endl;
                        send(sock2,"NOT FOUND",300,0);
                    }
                    else{

                        string responsevalue(reply2["value"].GetString());

                        send(sock2,responsevalue.c_str(),300,0);
                    }

                }
                else
                {
                    cout<<"Parsing successful"<<endl;
                    cout<<"Sent to USER"<<endl;
                    string responsevalue(reply1["value"].GetString());
                    send(sock2,responsevalue.c_str(),300,0);
                }

                //newRequest(key);
                cout<<"Cache updated successfully"<<endl;



                }



            }
        else if(strcmp(document["reqtype"].GetString(),"delreq")==0)
            {
                cout<<"This is Del Request"<<endl;

                string key(document["key"].GetString());

                int hashedkey=hashfunction(key);

                int slaveServerNumber=((hashedkey%numberOfSlaveServers)+1);

                cout<<slaveServerNumber<<endl;

                string ipPortCombination=slaveUidToipport[slaveServerNumber];

                string ipaddress=ipPortCombination.substr(0, ipPortCombination.find(":"));

                string portaddress=ipPortCombination.substr(ipPortCombination.find(":")+1);

                cout<<"IP "<<ipaddress<<" PORT "<<portaddress<<endl;





                int successor=(slaveServerNumber%numberOfSlaveServers)+1;


                string ipPortCombination1=slaveUidToipport[successor];

                string ipaddress1=ipPortCombination1.substr(0, ipPortCombination1.find(":"));

                string portaddress1=ipPortCombination1.substr(ipPortCombination1.find(":")+1);

                cout<<successor<<endl;
                int slavesock=uidToSocket[slaveServerNumber];
                int slavesocksuccessor=uidToSocket[successor];

                string getmessageinjson1 = preparePREPARESLAVEDELmessageinjson(key);
                send(slavesock,getmessageinjson1.c_str(),300,0);

                string getmessageinjson2 = preparePREPARESLAVEDELmessageinjson(key);
                send(slavesocksuccessor,getmessageinjson2.c_str(),300,0);

                char receiveResponse[300];


                recv(slavesock,receiveResponse,300,0);



                char receiveResponse1[300];

                recv(slavesocksuccessor,receiveResponse1,300,0);

                Document reply1,reply2;

                if (reply1.ParseInsitu(receiveResponse).HasParseError())
                {
                    cout<<"DocumentParsing error"<<endl;
                }
                else
                {
                    cout<<"Parsing successful"<<endl;

                if (reply2.ParseInsitu(receiveResponse1).HasParseError())
                {
                    cout<<"DocumentParsing error"<<endl;
                }
                else
                {
                    cout<<"Parsing successful"<<endl;
                    if(reply2["prepared"].GetInt()==1 && reply1["prepared"].GetInt()==1)
                    {
                             getmessageinjson1 = prepareCOMMITSLAVEmessageinjson(1);
                            send(slavesock,getmessageinjson1.c_str(),300,0);

                             getmessageinjson2 = prepareCOMMITSLAVEmessageinjson(1);
                            send(slavesocksuccessor,getmessageinjson2.c_str(),300,0);



                    }
                    else
                    {

                            getmessageinjson1 = prepareCOMMITSLAVEmessageinjson(0);
                            send(slavesock,getmessageinjson1.c_str(),300,0);

                             getmessageinjson2 = prepareCOMMITSLAVEmessageinjson(0);
                            send(slavesocksuccessor,getmessageinjson2.c_str(),300,0);
                    }

                }

                }





                cacheKeyValueMapping.erase(key);

                cout<<"Key Deleted Successfully"<<endl;

                send(sock2,"Key Deleted Successfully",256,0);
            }
        else if(strcmp(document["reqtype"].GetString(),"byereq")==0)
            {cout<<"Connection ends"<<endl;close(sock2);break;}

        }
    }

}


void *
timerbeat(void *udpport){
    sleep(30);
    while(true)
    {
        for(int i=1;i<=numberOfSlaveServers;i++)
        {
            if(timedout[i]==0)
                    {isLive[i]=false;cout<<"Falsed islive of:"<<i<<endl;}
            timedout[i]=0;

        }
        sleep(20);

    }

}



void *
listenbeat(void *udpport){
    int udpporta=*((int *)udpport);
    printf("staring udp server\n");

	int server_fd, new_socket, valread;
	struct sockaddr_in address;
	int opt = 1;
	socklen_t addrlen = sizeof(address);


	if ((server_fd = socket(AF_INET, SOCK_DGRAM,0)) == 0) // localhost for TCP/IP
	{
		printf("could not start server");
		exit(0);
	}

	address.sin_family = AF_INET;
	address.sin_addr.s_addr =  htonl(INADDR_ANY);
	address.sin_port = htons( udpporta );
    cout<<"Binding on "<<udpporta;

	if(bind(server_fd, (struct sockaddr *)&address,
				sizeof(address))<0)
	{
		perror("bind failed");
		exit(EXIT_FAILURE);
	}
//
//printf("started heart beat server successfully \n");'


    while(1)
	{
		char buf[1024]={0};

        cout<<"before read"<<endl;
//		printf("waiting on port %d\n", PORT);
		//recvfrom(server_fd, buf, 1-24, 0, (struct sockaddr *)&address, &addrlen);
        recv(server_fd, buf, 1024, 0);
        int beatClient=atoi(buf);
        timedout[beatClient]++;
        cout<<"receiving"<<endl;
		cout<<buf<<endl;


    }


}

int
main(int argc,char *argv[]){
    //cache starts


    //cache ends
    cout<<"Enter number of slave servers:"<<endl;
    cin>>numberOfSlaveServers;

    for(int i=1;i<=numberOfSlaveServers;i++)
    {
        timedout[i]=0;
    }

    char *port;
    if(argc!=3){
        error("Not Enough Arguments");
    }
    port=argv[2];
//starting server port and listening for connection

struct sockaddr_in server, client;
int sock1,sock2;
sock1=socket(AF_INET, SOCK_STREAM, 0);
if(sock1<0)
    error("Error Creating Socket");
bzero((char *)&server, sizeof(server));
server.sin_family=AF_INET;
server.sin_port=htons(atoi(port));
server.sin_addr.s_addr=INADDR_ANY;
if(bind(sock1, (struct sockaddr*)&server, sizeof(server))<0)
    error("Error Binding");
if(listen(sock1, 1)<0)
    error("Error Listen");
socklen_t clientLen = sizeof(client);
pthread_t udpthread;
int udpport=HEARTBEATPORT;
if(pthread_create(&udpthread,NULL,listenbeat,(void*)&udpport)<0){
    error("Thread error");
}


pthread_t timerthread;
//int udpport=8453;
if(pthread_create(&timerthread,NULL,timerbeat,(void*)&udpport)<0){
    error("Thread error");
}



pthread_t threadid;
while((sock2=accept(sock1, (struct sockaddr*)&client,&clientLen))>0)
{
    parameter node;
    char abuffer[200];
    char nodeType;
    bzero(abuffer, 200);
    recv(sock2, abuffer, 200, 0);
    string buffer(abuffer);


    //char buffer[jsonstring.length()];
    //memcpy(buffer, jsonstring.c_str(), jsonstring.length());
    //cout<<buffer<<endl;

    int pvalue,qvalue;
    if (document.ParseInsitu(abuffer).HasParseError())
    {
        cout<<"Error Parsing Document"<<endl;
        pvalue=0;
        qvalue=0;
    }
       // return;
    else if(strcmp(document["reqtype"].GetString(),"registerclientreq")==0)
    {
    assert(document.IsObject());

    assert(document.HasMember("clienthostid"));
    assert(document["clienthostid"].IsString());


    printf("\nParsing to document succeeded.\n");


    pvalue=currentclientuid;
    currentclientuid++;

    cout<<"This is clienthostid:"<<document["clienthostid"].GetString()<<endl;


    char result[100];
    strcpy(result,document["clienthostid"].GetString());
    strcat(result,":");
    strcat(result,(to_string(document["clientport"].GetInt())).c_str());
    string result2(result);
    clientUidToipport[pvalue]=result2;


    string ackstring=prepareREGISTERACKCLIENTmessageinjson(pvalue,globalclientport++);
    cout<<ackstring<<endl;
    send(sock2,ackstring.c_str(),300,0);

    /*size_t a=buffer.find("#");
    nodeType=(char)*(buffer.substr(0,a).c_str());
    cout<<nodeType<<endl;
    buffer=buffer.substr(a+1);
    cout<<buffer<<endl;
    node.sock2=sock2;
    strcpy(node.structBuffer,buffer.c_str());
    //node.structBuffer=(char *)buffer.c_str();
    if(nodeType!='0'){
        */
    if(pthread_create(&threadid,NULL,connectClient,(void*)&globalclientport)<0){
      error("Thread error");
    }

    }

    else if(strcmp(document["reqtype"].GetString(),"registerslavereq")==0)
    {

    assert(document.IsObject());

    assert(document.HasMember("slavehostid"));
    assert(document["slavehostid"].IsString());


    printf("\nParsing to document succeeded.\n");


    qvalue=currentslaveuid;
    currentslaveuid++;

    cout<<"This is slavehostid:"<<document["slavehostid"].GetString()<<endl;
    cout<<"This is slaveport:"<<document["slaveport"].GetInt()<<endl;

    char result[100];
    strcpy(result,document["slavehostid"].GetString());
    strcat(result,":");
    strcat(result,(to_string(document["slaveport"].GetInt())).c_str());
    string result2(result);
    cout<<result2<<endl;
    slaveUidToipport[qvalue]=result2;
    string ackstring;
    if(ipportToUid.find(result2)!=ipportToUid.end())
    {
        ackstring=prepareREGISTERACKSLAVEmessageinjson(ipportToUid[result2],globalslaveport++);
    }

    else
    {
        ipportToUid[result2]=qvalue;
        ackstring=prepareREGISTERACKSLAVEmessageinjson(qvalue,globalslaveport++);

    }
    cout<<ackstring<<endl;
    send(sock2,ackstring.c_str(),300,0);

    isLive[qvalue]=true;
    timedout[qvalue]=1;



    parameter node;
    node.slavePort=globalslaveport;
    node.slaveUid=qvalue;
    /*size_t a=buffer.find("#");
    nodeType=(char)*(buffer.substr(0,a).c_str());
    cout<<nodeType<<endl;
    buffer=buffer.substr(a+1);
    cout<<buffer<<endl;
    node.sock2=sock2;
    strcpy(node.structBuffer,buffer.c_str());
    //node.structBuffer=(char *)buffer.c_str();
    if(nodeType!='0'){
        */

    if(pthread_create(&threadid,NULL,connectSlave,(void*)&node)<0){
      error("Thread error");
    }

    }

    else
    {
        cout<<"Error in received request from neither a slave nor a client"<<endl;
    }
    //}
    /*else{
        if(pthread_create(&threadid,NULL,connectClient,(void*)&node)<0){
        error("Thread error");
        }
    }*/
    close(sock2);
}
return 0;
}
