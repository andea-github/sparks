package demo.utils.voice;

import com.jacob.activeX.ActiveXComponent;
import com.jacob.com.Dispatch;
import com.jacob.com.Variant;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

public class WordToVoice {

    public static void main(String[] args) {
        // 创建与微软应用程序的新连接。传入的参数是注册表中注册的程序的名称。
        ActiveXComponent sap = new ActiveXComponent("Sapi.SpVoice");
        try {
            // 音量 0-100
            sap.setProperty("Volume", new Variant(100));
            // 语音朗读速度 -10 到 +10
            sap.setProperty("Rate", new Variant(3));
            // 获取执行对象
            Dispatch sapo = sap.getObject();
            // 执行朗读
            Dispatch.call(sapo, "Speak", new Variant("good morning Jack"));
            // 关闭执行对象
            sapo.safeRelease();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            // 关闭应用程序连接
            sap.safeRelease();
        }
    }

    public static void jacobText(String path) throws Exception {
        ActiveXComponent sap = new ActiveXComponent("Sapi.SpVoice");
        // 输入文件
        File srcFile = new File(path);
        // 使用包装字符流读取文件
        BufferedReader br = new BufferedReader(new FileReader(srcFile));
        String content = br.readLine();
        try {
            // 音量 0-100
            sap.setProperty("Volume", new Variant(100));
            // 语音朗读速度 -10 到 +10
            sap.setProperty("Rate", new Variant(0));
            // 获取执行对象
            Dispatch sapo = sap.getObject();
            // 执行朗读
            while (content != null) {
                Dispatch.call(sapo, "Speak", new Variant(content));
                content = br.readLine();
            }
            // 关闭执行对象
            sapo.safeRelease();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            br.close();
            // 关闭应用程序连接
            sap.safeRelease();
        }
    }
}
