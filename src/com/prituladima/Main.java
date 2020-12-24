package com.prituladima;

import com.prituladima.io.InputReader;
import com.prituladima.io.NIOInputReader;
import com.prituladima.io.OutputWriter;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.ScatteringByteChannel;

import static com.prituladima.Constants.FILE_SIZE;

public class Main {
    private static File srcFile = new File("src.txt");
    private static File desFile = new File("des.txt");
    private static int bufferSize = 8192;

    public static void main(String[] args) throws Exception {
        double aver = 0;

        int n = 5;
        for (int i = 0; i < n; i++) {
            long io;
            long cusIO;
            long cusNIO;
            long scatterGather;
            {
                long start = System.currentTimeMillis();
                // 1. IO
//                io();
                long end = System.currentTimeMillis();
                io = end - start;
            }
            {
                long start = System.currentTimeMillis();
                // 2. IO
                cusIO();
                long end = System.currentTimeMillis();
                cusIO = end - start;
            }
            {
                long start = System.currentTimeMillis();
                // 2. IO
                cusNIO();
                long end = System.currentTimeMillis();
                cusNIO = end - start;
            }
            {
                long start = System.currentTimeMillis();
                // 3. Scatter/Gather
//                scatterGather();
                long end = System.currentTimeMillis();
                scatterGather = end - start;
            }
            System.out.printf("io time is [%s] ms\n", io);
            System.out.printf("cusIO time is [%s] ms\n", cusIO);
            System.out.printf("cusNIO time is [%s] ms\n", cusNIO);
            System.out.printf("scatterGather time is [%s] ms\n", scatterGather);
//            System.out.printf("Times [%s]\n", io / scatterGather);
            if (1 < i) aver += cusIO * 1.0/ cusNIO;
        }
        System.out.printf("Summary average [%s]\n", aver / (n - 2));
    }

    public static void io() throws Exception {
        FileInputStream fis = new FileInputStream(srcFile);
        FileOutputStream fos = new FileOutputStream(desFile);

        BufferedInputStream bis = new BufferedInputStream(fis, bufferSize);
        BufferedOutputStream bos = new BufferedOutputStream(fos, bufferSize);

        int read = -1;
        while ((read = bis.read()) != -1) {
            //bos.write(read);
        }

        bos.close();
        bis.close();
    }

    public static void cusIO() throws Exception {
        InputReader fis = new InputReader(new FileInputStream(srcFile));
        //OutputWriter fos = new OutputWriter(new FileOutputStream(desFile));

        try {
            while (true) {
                fis.readString();
                //fos.print(fis.read());
                //fos.print('\n');
            }
        } catch (Exception e) {
//            System.out.println("EOF");
        }

    }

    public static void cusNIO() throws Exception {
        NIOInputReader fis = new NIOInputReader(new FileInputStream(srcFile));
        //OutputWriter fos = new OutputWriter(new FileOutputStream(desFile));

        try {
            while (true) {
                fis.readString();
                //fos.print(fis.read());
                //fos.print('\n');
            }
        } catch (Exception e) {
//            System.out.println("EOF");
        }

    }

    public static void scatterGather() throws Exception {
        try (FileInputStream fis = new FileInputStream(srcFile);
             //FileOutputStream fos = new FileOutputStream(desFile);

             ScatteringByteChannel sbc = fis.getChannel();) {
            //GatheringByteChannel gbc = fos.getChannel();

            ByteBuffer bb = ByteBuffer.allocateDirect(bufferSize);
            while (sbc.read(bb) != -1) {
                bb.flip();//bb.get(new byte[0]);
//            gbc.write(bb);
                bb.clear();
            }
        }
        //fos.close();
        //fis.close();

    }


}
