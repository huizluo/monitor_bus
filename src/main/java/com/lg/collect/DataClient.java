package com.lg.collect;

import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.BasicResponseHandler;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class DataClient {
    //调配编号
    static String[] deployArr = {"316d5c75-e860-4cc9-a7de-ea2148c244a0",
            "32102c12-6a73-4e03-80ab-96175a8ee686",
            "a97f6c0d-9086-4c68-9d24-8a7e89f39e5a",
            "adfgfdewr-5463243546-4c68-9d24-8a7e8",
            };
    //sim卡号
    static String[] simArr = {"1111", "2222", "3333", "4444"};
    //道路运输证
    static String[] transpotNumArr = {"ysz11111", "ysz22222","ysz333333","ysz44444"};
    //车牌号
    static String[] plateNumArr = {"京A-11111", "京A-22222", "京A-33333", "京A-44444"};
    //时间static
    static String[] timeStrArr = {"1594076827", "1594076527", "1594076327"};
    //经纬度
    static String[] lglatArr = {"116.437355_39.989739",
            "116.382306_39.960325",
            "116.623784_40.034688",
            "116.32139_39.81157",
            "116.45551_39.944381",};
    //速度
    static String[] speedArr = {"50", "60", "70", "80"};
    //方向
    static String[] directionArr = {"west", "east", "south", "north"};
    //里程
    static String[] mileageArr = {"6000", "7000", "8000", "9000"};
    //剩余油量
    static String[] oilRemainArr = {"20", "30", "70", "80"};
    //载重质量
    static String[] weightsArr = {"500", "1000", "2000", "3000"};
    //ACC开关
    static String[] accArr = {"0", "1"};
    //是否定位
    static String[] locateArr = {"0", "1"};
    //车辆油路是否正常
    static String[] oilWayArr = {"0", "1"};
    //车辆电路是否正常
    static String[] electricArr = {"0", "1"};


    /**
     * @param url
     * @param msg
     * @return
     */
    public static String httpPost(String url, String msg) {
        String returnValue = "这是默认返回值，接口调用失败";
        CloseableHttpClient httpClient = HttpClients.createDefault();
        ResponseHandler<String> responseHandler = new BasicResponseHandler();
        try {
            //第一步：创建HttpClient对象
            httpClient = HttpClients.createDefault();
            //第二步：创建httpPost对象
            HttpPost httpPost = new HttpPost(url);
            //第三步：给httpPost设置JSON格式的参数
            StringEntity requestEntity = new StringEntity(msg, "utf-8");
            requestEntity.setContentEncoding("UTF-8");
            httpPost.setEntity(requestEntity);
            //第四步：发送HttpPost请求，获取返回值
            httpClient.execute(httpPost, responseHandler);

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                httpClient.close();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        //第五步：处理返回值
        return returnValue;
    }

    public static void main(String[] args) throws InterruptedException {
        String url = "http://node51/log/lagou_bus_info";
        int n = 100;
        final Random rd = new Random();
        while (n > 0) {
            //拼接信息
            final StringBuilder sb = new StringBuilder();
            sb.append(deployArr[rd.nextInt(deployArr.length)]).append(",");
            sb.append(simArr[rd.nextInt(simArr.length)]).append(",");
            sb.append(transpotNumArr[rd.nextInt(transpotNumArr.length)]).append(",");
            sb.append(plateNumArr[rd.nextInt(plateNumArr.length)]).append(",");
            sb.append(lglatArr[rd.nextInt(lglatArr.length)]).append(",");
            sb.append(speedArr[rd.nextInt(speedArr.length)]).append(",");
            sb.append(directionArr[rd.nextInt(directionArr.length)]).append(",");
            sb.append(mileageArr[rd.nextInt(mileageArr.length)]).append(",");
            sb.append(timeStrArr[rd.nextInt(timeStrArr.length)]).append(",");
            sb.append(oilRemainArr[rd.nextInt(oilRemainArr.length)]).append(",");
            sb.append(weightsArr[rd.nextInt(weightsArr.length)]).append(",");
            sb.append(accArr[rd.nextInt(accArr.length)]).append(",");
            sb.append(locateArr[rd.nextInt(locateArr.length)]).append(",");
            sb.append(oilWayArr[rd.nextInt(oilWayArr.length)]).append(",");
            sb.append(electricArr[rd.nextInt(electricArr.length)]);

            httpPost(url, sb.toString());
            TimeUnit.SECONDS.sleep(1);
            n--;
        }


    }
}
