package edu.umkc.sce.io;

import java.io.IOException;
import java.io.OutputStream;
import java.security.MessageDigest;

public class HashedOutputStream extends OutputStream {
	private final MessageDigest digest;
	private final OutputStream wrapped;

	public HashedOutputStream(MessageDigest digest, OutputStream wrapped) {
		this.digest = digest;
		this.wrapped = wrapped;
	}
	

	@Override
	public void write(byte[] b) throws IOException {
		wrapped.write(b);
		digest.update(b);
	}

	@Override
	public void write(byte[] b, int off, int len) throws IOException {
		wrapped.write(b);
		digest.update(b, off, len);
	}

	@Override
	public void write(int b) throws IOException {
		wrapped.write(b);
		byte[] ret = new byte[4];
		ret[0] = (byte) (b & 0xFF);
		ret[1] = (byte) ((b >> 8) & 0xFF);
		ret[2] = (byte) ((b >> 16) & 0xFF);
		ret[3] = (byte) ((b >> 24) & 0xFF);
		digest.update(ret[3]);

	}

}
