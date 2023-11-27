package com.rogerguo.test.motivation;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;

import java.util.ArrayList;
import java.util.List;

/**
 * @author yangguo
 * @create 2021-10-19 2:27 PM
 **/
public class MultipartUpload {

    public static void test() {
    }


    public static List<UploadPartResponse> uploadParts(S3Client s3Client, String bucketName, String key, String uploadId, List<String> contentsToUpload) {

        List<UploadPartResponse> uploadPartResponses = new ArrayList<>();

        for (int i = 0; i < contentsToUpload.size(); i++) {
            int partNumber = i + 1;
            UploadPartRequest uploadPartRequest = UploadPartRequest.builder().bucket(bucketName).key(key).uploadId(uploadId)
                    .partNumber(partNumber).build();
            UploadPartResponse uploadPartResponse = s3Client.uploadPart(uploadPartRequest, RequestBody.fromString(contentsToUpload.get(i)));
            uploadPartResponses.add(uploadPartResponse);
        }

        return uploadPartResponses;
    }

}
