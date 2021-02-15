package com.prituladima;

import java.io.*;
import java.util.Iterator;
import java.util.StringTokenizer;

@Deprecated
public final class BufferedScanner implements Iterator<String>, AutoCloseable {
    private static final StringTokenizer EOF = new StringTokenizer("");
    private StringTokenizer tokenizer;
    private BufferedReader reader;

    private BufferedScanner(File file) throws IOException {
        this.reader = new BufferedReader(new FileReader(file));
    }

    @Override
    public boolean hasNext() {
        updateTokenizer();
        return tokenizer != EOF && tokenizer.hasMoreTokens();
    }

    @Override
    public String next() {
        updateTokenizer();
        return tokenizer.nextToken();
    }

    private void updateTokenizer() {
        while (tokenizer == null || !tokenizer.hasMoreTokens()) {
            try {
                final String nextLineToTokenize = reader.readLine();
                if (nextLineToTokenize != null)
                    tokenizer = new StringTokenizer(nextLineToTokenize);
                else {
                    tokenizer = EOF;
                    break;
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }


    @Override
    public void close() throws IOException {
        this.reader.close();
    }
}

