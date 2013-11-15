package com.peerindex.s3stream;


/**
 * @author Enno Shioji (enno.shioji@peerindex.com)
 */
class PartTicket {
    final int partNum;
    final long uploadedBytesSofar;

    PartTicket(int partNum, long uploadedBytesSofar) {
        this.partNum = partNum;
        this.uploadedBytesSofar = uploadedBytesSofar;
    }
}
