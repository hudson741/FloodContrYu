namespace java com.floodCtr.generate

service  FloodContrThriftService {

   string getStormUi(),

   string getStormNimbus(),

   string getAllDockerJob(),

   void addDockerComponent(1: string imageName,  2: string containerName, 3: string dockerIp, 4:string dockerArgs, 5:string netUrl , 6:map<string,string> host, 7:map<string,string> port)  

}
