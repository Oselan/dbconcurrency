using Npgsql;

public class Demo{

    private int count;
    private static string lockObject = "lockObject";

    public Demo(int count=1){
        this.count=count;
    }

    private void synchronizedAddDocument(NpgsqlConnection con, int departmentId, string content){
        lock(lockObject){
            addDocument(con, departmentId, content);
        }
    }

    private void addDocumentOptimisticLock(NpgsqlConnection con, int departmentId, string content){
        con.Open();
        NpgsqlTransaction tr=con.BeginTransaction();
        NpgsqlCommand cmd =new NpgsqlCommand("SELECT serial_num,code,version FROM department where id=@departmentId", con);
        cmd.Parameters.AddWithValue("departmentId", departmentId);
        int? id=null;
        string? code=null;
        int? version=null;
        using(NpgsqlDataReader reader = cmd.ExecuteReader()){
            if(reader.Read()){
                id=reader.GetInt32(0);
                code=reader.GetString(1);
                version=reader.GetInt32(2);
            }
            else{
                tr.Rollback();
            }
            reader.Close();
        }
        if (id!=null){
            cmd=new NpgsqlCommand("update department set serial_num=serial_num+1, version=version+1 where id=@departmentId and version=@version", con);
            cmd.Parameters.AddWithValue("departmentId", departmentId);
            cmd.Parameters.AddWithValue("version", version!);
            if (cmd.ExecuteNonQuery()>0){
                cmd = new NpgsqlCommand("INSERT INTO document(department_id, reference, content) VALUES(@departmentId, @reference, @content)", con);
                cmd.Parameters.AddWithValue("departmentId", departmentId);
                cmd.Parameters.AddWithValue("reference", code+"/"+id+"/"+DateTime.Now.Year.ToString());
                cmd.Parameters.AddWithValue("content", content);
                cmd.ExecuteNonQuery();
            }
            else{
                // Should retry
            }
        }
        tr.Commit();
        con.Close();
    }
    
    private void addDocumentWithDBRowLevelLock(NpgsqlConnection con, int departmentId, string content){
        con.Open();
        NpgsqlTransaction tr=con.BeginTransaction();
        NpgsqlCommand cmd =new NpgsqlCommand("SELECT serial_num,code FROM department where id=@departmentId for update", con);
        cmd.Parameters.AddWithValue("departmentId", departmentId);
        int? id=null;
        string? code=null;
        using(NpgsqlDataReader reader = cmd.ExecuteReader()){
            if(reader.Read()){
                id=reader.GetInt32(0);
                code=reader.GetString(1);
            }
            else{
                tr.Rollback();
            }
            reader.Close();
        }
        if (id!=null){
            cmd=new NpgsqlCommand("update department set serial_num=serial_num+1 where id=@departmentId", con);
            cmd.Parameters.AddWithValue("departmentId", departmentId);
            cmd.ExecuteNonQuery();
            cmd = new NpgsqlCommand("INSERT INTO document(department_id, reference, content) VALUES(@departmentId, @reference, @content)", con);
            cmd.Parameters.AddWithValue("departmentId", departmentId);
            cmd.Parameters.AddWithValue("reference", code+"/"+id+"/"+DateTime.Now.Year.ToString());
            cmd.Parameters.AddWithValue("content", content);
            cmd.ExecuteNonQuery();
        }
        tr.Commit();
        con.Close();
    }


    private void addDocument(NpgsqlConnection con, int departmentId, string content){
        con.Open();
        NpgsqlTransaction tr=con.BeginTransaction();
        NpgsqlCommand cmd =new NpgsqlCommand("SELECT serial_num,code FROM department where id=@departmentId", con);
        cmd.Parameters.AddWithValue("departmentId", departmentId);
        int? id=null;
        string? code=null;
        using(NpgsqlDataReader reader = cmd.ExecuteReader()){
            if(reader.Read()){
                id=reader.GetInt32(0);
                code=reader.GetString(1);
            }
            else{
                tr.Rollback();
            }
            reader.Close();
        }
        if (id!=null){
            cmd=new NpgsqlCommand("update department set serial_num=serial_num+1 where id=@departmentId", con);
            cmd.Parameters.AddWithValue("departmentId", departmentId);
            cmd.ExecuteNonQuery();
            cmd = new NpgsqlCommand("INSERT INTO document(department_id, reference, content) VALUES(@departmentId, @reference, @content)", con);
            cmd.Parameters.AddWithValue("departmentId", departmentId);
            cmd.Parameters.AddWithValue("reference", code+"/"+id+"/"+DateTime.Now.Year.ToString());
            cmd.Parameters.AddWithValue("content", content);
            cmd.ExecuteNonQuery();
        }
        tr.Commit();
        con.Close();
    }
    public void run(){
        NpgsqlConnection con= new NpgsqlConnection("Host=localhost;Username=dany;Database=demo1");
        for(int i=0;i<count;++i){
            addDocument(con, 1+(i%3), "Document "+i);
        }
        con.Close();
    }

    public void runWithApplicationSync(){
        NpgsqlConnection con= new NpgsqlConnection("Host=localhost;Username=dany;Database=demo1");
        for(int i=0;i<count;++i){
            synchronizedAddDocument(con, 1+(i%3), "Document "+i);
        }
        con.Close();
    }

    public void runWithDBRowLevelLock(){
        NpgsqlConnection con= new NpgsqlConnection("Host=localhost;Username=dany;Database=demo1");
        for(int i=0;i<count;++i){
            addDocumentWithDBRowLevelLock(con, 1+(i%3), "Document "+i);
        }
        con.Close();
    }
    public void runWithOptimistic(){
        NpgsqlConnection con= new NpgsqlConnection("Host=localhost;Username=dany;Database=demo1");
        for(int i=0;i<count;++i){
            addDocumentOptimisticLock(con, 1+(i%3), "Document "+i);
        }
        con.Close();
    }

    public static void cleanup(){
        NpgsqlConnection con= new NpgsqlConnection("Host=localhost;Username=dany;Password=mypassword;Database=demo1");
        con.Open();
        NpgsqlCommand cmd =new NpgsqlCommand("DELETE FROM document", con);
        cmd.ExecuteNonQuery();
        cmd =new NpgsqlCommand("update department set serial_num=1", con);
        cmd.ExecuteNonQuery();
        con.Close();
    }
}

public class MainProgram{
    private static void sequentialTest(){
        Demo d = new Demo(50);
        d.run();    
    }  

    private static void parallelTestNoSync(){
        try{
            Thread[] t=new Thread[10];
            for(int i=0;i<10;++i){
                t[i]=new Thread(new Demo(5).run);
                t[i].Start();
            }
            for(int i=0;i<10;++i){
                t[i].Join();
            }
        }
        catch(Exception e){
            Console.WriteLine("Error:{0}", e.Message);
        }
    } 

    private static void parallelTestWithApplicationSync(){
        try{
            Thread[] t=new Thread[10];
            for(int i=0;i<10;++i){
                t[i]=new Thread(new Demo(5).runWithApplicationSync);
                t[i].Start();
            }
            for(int i=0;i<10;++i){
                t[i].Join();
            }
        }
        catch(Exception e){
            Console.WriteLine("Error:{0}", e.Message);
        }
    }

    private static void parallelTestWithDBRowLevelLock(){
        try{
            Thread[] t=new Thread[10];
            for(int i=0;i<10;++i){
                t[i]=new Thread(new Demo(5).runWithDBRowLevelLock);
                t[i].Start();
            }
            for(int i=0;i<10;++i){
                t[i].Join();
            }
        }
        catch(Exception e){
            Console.WriteLine("Error:{0}", e.Message);
        }
    }

    private static void parallelTestWithOptimisticLock(){
        try{
            Thread[] t=new Thread[10];
            for(int i=0;i<10;++i){
                t[i]=new Thread(new Demo(5).runWithOptimistic);
                t[i].Start();
            }
            for(int i=0;i<10;++i){
                t[i].Join();
            }
        }
        catch(Exception e){
            Console.WriteLine("Error:{0}", e.Message);
        }
    }


    public static void Main(string[] args){
        Dictionary<string,Delegate> tests = new Dictionary<string,Delegate>();
        tests.Add("1. sequentialTest", new Action(sequentialTest));
        tests.Add("2. parallelTestNoSync", new Action(parallelTestNoSync));
        tests.Add("3. parallelTestWithApplicationSync", new Action(parallelTestWithApplicationSync));
        tests.Add("4. parallelTestWithDBRowLevelLock", new Action(parallelTestWithDBRowLevelLock));
        tests.Add("5. parallelTestWithOptimisticLock", new Action(parallelTestWithOptimisticLock));
        while(true){
            foreach(var kv in tests){
                Console.WriteLine(kv.Key);
            }
            Console.WriteLine("Enter the test to run:");
            string t=Console.ReadLine() ?? "";
            string? k=tests.Keys.FirstOrDefault(x=>x.StartsWith(t));
            if(k != null){
                Demo.cleanup();
                tests[k].DynamicInvoke();
            }
            else{
                Console.WriteLine("Invalid test");
            }
        }
    }
}
