package com.jxq;
import org.apache.commons.httpclient.HostConfiguration;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.apache.commons.httpclient.methods.GetMethod;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ReceiveMessageServiceTest {
    private MultiThreadedHttpConnectionManager httpConnManager;
    private HttpClient httpClient;
    private List<String> streamingUrlList=new ArrayList<String>();
    private int curStreamUrlIndex;
    public static long lastMsgLocation = -1L;
    private DataInputStream inputStream;
    private final int recBufSize = 256;
    private byte[] recBuf;
    private int recIndex;

    /**
     * 获取数据主线程
     *
     * @param args
     */
    public static void main(String[] args) {
        ReceiveMessageServiceTest test = new ReceiveMessageServiceTest();
        test.init();
    }

    /**
     * 初始化httpclient，并启动获取数据线程
     */
    public void init() {
        this.streamingUrlList.add("http://c.api.weibo.com/datapush/comment?subid=10823");
        this.httpConnManager = new MultiThreadedHttpConnectionManager();
        this.httpConnManager.getParams().setMaxConnectionsPerHost(HostConfiguration.ANY_HOST_CONFIGURATION, 10);
        this.httpConnManager.getParams().setMaxTotalConnections(10);
        this.httpConnManager.getParams().setSoTimeout(2147483647);
        this.httpConnManager.getParams().setConnectionTimeout(10000);
        this.httpConnManager.getParams().setReceiveBufferSize(655350);
        this.httpClient = new HttpClient(this.httpConnManager);
        new ReadTask().start();
    }

    /**
     * 获取数据线程
     *
     */
    class ReadTask extends Thread {

        /**
         * 启一个线程从服务器读取数据
         */
        public void run() {
            while (true) {
                GetMethod method = null;
                recIndex = 0;
                recBuf = new byte[recBufSize];
                try {
                    method = connectStreamServer();
                    while (true) {
                        processLine();
                    }
                } catch (Exception e) {
                    // 当连接断开时，重新连接
                    System.out.println("streaming process error " + e.getMessage());
                    lastMsgLocation = lastMsgLocation + 2;
                    curStreamUrlIndex = ++curStreamUrlIndex % streamingUrlList.size();
                } finally {
                    if (method != null)
                        method.releaseConnection();
                }
            }
        }

        /**
         * 建立http连接
         *
         * @return
         */
        private GetMethod connectStreamServer() throws IOException {
            String targetURL = (String) streamingUrlList.get(curStreamUrlIndex);
            // 从指定的since_id开始读取数据，保证读取数据的连续性，消息完整性
            if (lastMsgLocation > 0L) {
                targetURL = targetURL + "&since_id=" + lastMsgLocation;
            }
            System.out.println("StreamingReceiver http get url=" + targetURL);
            GetMethod method = new GetMethod(targetURL);
            int statusCode = 0;
            statusCode = httpClient.executeMethod(method);

            if (statusCode != HttpStatus.SC_OK) {
                throw new RuntimeException(".. ");
            }

            try {
                inputStream = new DataInputStream(method.getResponseBodyAsStream());
            } catch (IOException e) {
                throw new RuntimeException("get stream input io exception", e);
            }

            return method;
        }

        /**
         * 读取并处理数据
         *
         * @throws IOException
         */
        private void processLine() throws IOException {
            byte[] bytes = readLineBytes();
            if ((bytes != null) && (bytes.length > 0)) {
                String message = new String(bytes);
                handleMessage(message);
            }
        }

        /**
         * 可以重写此方法解析message，同时需将lastMsgLocation赋值为最近1条消息的id
         *
         * @param message
         */
        private void handleMessage(String message) {
            System.out.println(message);
        }

        /**
         * 读取数据
         *
         * @return
         * @throws IOException
         */
        public byte[] readLineBytes() throws IOException {
            byte[] result = null;
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            int readCount = 0;
            if ((recIndex > 0) && (read(bos))) {
                return bos.toByteArray();
            }
            while ((readCount = inputStream.read(recBuf, recIndex, recBuf.length - recIndex)) > 0) {
                recIndex = (recIndex + readCount);
                if (read(bos)) {
                    break;
                }
            }
            result = bos.toByteArray();
            if ((result == null) || ((result != null) && (result.length <= 0) && (recIndex <= 0))) {
                throw new IOException("++++ Stream appears to be dead, so closing it down");
            }
            return result;
        }

        /**
         * 读数据到bos
         *
         * @param bos
         * @return
         */
        private boolean read(ByteArrayOutputStream bos) {
            boolean result = false;
            int index = -1;
            for (int i = 0; i < recIndex - 1; i++) {
                // 13cr-回车 10lf-换行
                if ((recBuf[i] == 13) && (recBuf[(i + 1)] == 10)) {
                    index = i;
                    break;
                }
            }
            if (index >= 0) {
                bos.write(recBuf, 0, index);
                byte[] newBuf = new byte[recBufSize];
                if (recIndex > index + 2) {
                    System.arraycopy(recBuf, index + 2, newBuf, 0, recIndex - index - 2);
                }
                recBuf = newBuf;
                recIndex = (recIndex - index - 2);
                result = true;
            } else if (recBuf[(recIndex - 1)] == 13) {
                bos.write(recBuf, 0, recIndex - 1);
                Arrays.fill(recBuf, (byte) 0);
                recBuf[0] = 13;
                recIndex = 1;
            } else {
                bos.write(recBuf, 0, recIndex);
                Arrays.fill(recBuf, (byte) 0);
                recIndex = 0;
            }

            return result;
        }
    }

}
