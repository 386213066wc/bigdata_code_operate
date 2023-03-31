package cn.flink.demo20;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class CheckIPChangeWithCepByJava {
    public static void main(String[] args) throws Exception {

        //todo: 1、构建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //todo: 2、接受socket数据
        DataStreamSource<String> socketTextStream = env.socketTextStream("bigdata01", 9999);

        //todo: 3、数据处理分组
        KeyedStream<LoginInfo, Tuple> keyedStream = socketTextStream.map(new MapFunction<String, LoginInfo>() {
            public LoginInfo map(String value) throws Exception {
                String[] users = value.split(",");

                return new LoginInfo(users[0], users[1], users[2], users[3]);
            }
        }).keyBy("username");


        //定义pattern，表示第一次操作的ip与第二次操作的ip不相等
        //todo: 4、定义Parttern
        Pattern<LoginInfo, LoginInfo> pattern = Pattern.<LoginInfo>begin("start").where( new SimpleCondition<LoginInfo>() {
            public boolean filter(LoginInfo value) throws Exception {
                if (value.username != null) {
                    return true;
                }
                return false;
            }
        }).next("second").where(new IterativeCondition<LoginInfo>() {
            @Override
            public boolean filter(LoginInfo second, Context<LoginInfo> ctx) throws Exception {

                Iterable<LoginInfo> start = ctx.getEventsForPattern("start");
                Iterator<LoginInfo> userLoginIterator = start.iterator();
                while (userLoginIterator.hasNext()) {
                    LoginInfo userLogin = userLoginIterator.next();
                    if (!second.ip.equals(userLogin.ip)) {
                        return true;
                    }
                }
                return false;
            }
        });

        //todo: 5、将Parttern应用到事件流中进行检测，同时指定时间类型
        PatternStream<LoginInfo> patternStream = CEP.pattern(keyedStream, pattern)
                .inProcessingTime();

        //todo: 6、提取匹配到的数据
        SingleOutputStreamOperator<LoginInfo> selectStream = patternStream.select(new PatternSelectFunction<LoginInfo, LoginInfo>() {

            public LoginInfo select(Map<String, List<LoginInfo>> patternMap) throws Exception {

                //Map[String, util.List[(String, UserLoginInfo)]]
                //todo: key就是定义规则的名称：start  second
                //todo: value就是满足对应规则的数据

                List<LoginInfo> start = patternMap.get("start");
                List<LoginInfo> second = patternMap.get("second");

                LoginInfo startData = start.iterator().next();
                LoginInfo secondData = second.iterator().next();

                System.out.println("满足start模式中的数据 ：" + startData);
                System.out.println("满足second模式中的数据：" + secondData);

                return secondData;
            }
        });

        selectStream.print();


        //todo: 7、提交任务
        env.execute("CheckIPChangeWithCepByJava");
    }


    //todo:定义用户登录信息实体对象
    public static   class LoginInfo {
        public  String ip;
        public  String username;
        public  String operateUrl;
        public  String time;

        //无参构造必须带上
        public LoginInfo() {}

        public LoginInfo(String ip, String username, String operateUrl, String time) {
            this.ip = ip;
            this.username = username;
            this.operateUrl = operateUrl;
            this.time = time;
        }

        @Override
        public String toString() {
            return ip+"\t"+username+"\t"+operateUrl+"\t"+time;
        }
    }
}

