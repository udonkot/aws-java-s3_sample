/*
 * Copyright 2010-2013 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package com.amazonaws.samples;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.UUID;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

/**
 * AWS S3動作確認クラス
 */
public class S3Sample {

	/**
	 * メイン
	 * @param args
	 * @throws IOException
	 */
    public static void main(String[] args) throws IOException {

    	//AmazonS3クラスのインスタンス生成
        AmazonS3 s3 = new AmazonS3Client();

        // リージョン指定
        // 東京はNorthEastに当たるので該当するEnumを指定
        Region usWest2 = Region.getRegion(Regions.AP_NORTHEAST_1);
        s3.setRegion(usWest2);

        // s3バケット名を指定
        // ここで指定した先のオブジェクト(ファイル)を操作する
        String bucketName = "ig-s3-test-bucket";
        String key = "MyObjectKey2";

        System.out.println("===========================================");
        System.out.println("Getting Started with Amazon S3");
        System.out.println("===========================================\n");

        String type = null;
        if(args.length == 1) {
        	// 引数を設定
        	type = args[0];
        } else {
        	// 引数指定がない場合は固定値を設定
        	type = "1";
        }



        try {
            if ("1".equals(type)) {
                // ダウンロード処理
                System.out.println("Listing buckets");
                for (Bucket bucket : s3.listBuckets()) {
                    System.out.println(" - " + bucket.getName());
                }
                System.out.println();

                // ダウンロードファイル取得
                System.out.println("Downloading an object");
                S3Object object = s3.getObject(new GetObjectRequest(bucketName, key));
                System.out.println("Content-Type: " + object.getObjectMetadata().getContentType());
                // ファイル出力
                displayTextInputStream(object.getObjectContent());

            } else if ("2".equals(type)) {
                // アップロード処理
                System.out.println("Uploading a new object to S3 from a file\n");
                key = "uploadFile" + UUID.randomUUID().toString();
                // ファイルアップロード
                s3.putObject(new PutObjectRequest(bucketName, key, createSampleFile()));

            } else {
                // バケット作成
                System.out.println("Creating bucket " + bucketName + "\n");
                s3.createBucket(bucketName);
            }
        } catch (AmazonServiceException ase) {
            System.out.println("Caught an AmazonServiceException, which means your request made it "
                    + "to Amazon S3, but was rejected with an error response for some reason.");
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
        } catch (AmazonClientException ace) {
            System.out.println("Caught an AmazonClientException, which means the client encountered "
                    + "a serious internal problem while trying to communicate with S3, "
                    + "such as not being able to access the network.");
            System.out.println("Error Message: " + ace.getMessage());
        }
    }

//            System.out.println("Listing objects");
//            ObjectListing objectListing = s3.listObjects(new ListObjectsRequest()
//                    .withBucketName(bucketName)
//                    .withPrefix("My"));
//            for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
//                System.out.println(" - " + objectSummary.getKey() + "  " +
//                        "(size = " + objectSummary.getSize() + ")");
//            }
//            System.out.println();

//            System.out.println("Deleting bucket " + bucketName + "\n");
//            s3.deleteBucket(bucketName);
//    }

    /**
     * Creates a temporary file with text data to demonstrate uploading a file
     * to Amazon S3
     *
     * @return A newly created temporary file with text data.
     *
     * @throws IOException
     */
    private static File createSampleFile() throws IOException {
        File file = File.createTempFile("aws-java-sdk-", ".txt");
        file.deleteOnExit();

        Writer writer = new OutputStreamWriter(new FileOutputStream(file));
        writer.write("abcdefghijklmnopqrstuvwxyz\n");
        writer.write("01234567890112345678901234\n");
        writer.write("!@#$%^&*()-=[]{};':',.<>/?\n");
        writer.write("01234567890112345678901234\n");
        writer.write("abcdefghijklmnopqrstuvwxyz\n");
        writer.close();

        return file;
    }

    /**
     * Displays the contents of the specified input stream as text.
     *
     * @param input
     *            The input stream to display as text.
     *
     * @throws IOException
     */
    private static void displayTextInputStream(InputStream input) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(input));
        while (true) {
            String line = reader.readLine();
            if (line == null) break;

            System.out.println("    " + line);
        }
        System.out.println();
    }

}
