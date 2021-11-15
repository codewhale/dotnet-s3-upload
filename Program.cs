using System;
using System.Threading.Tasks;

// To interact with Amazon S3.
using Amazon.S3;
using Amazon.S3.Model;

using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Amazon;

using System.Collections.Generic;

namespace S3CreateAndList
{
  class Program
  {
    // Main method
    static async Task Main(string[] args)
    {
      // Before running this app:
      // - Credentials must be specified in an AWS profile. If you use a profile other than
      //   the [default] profile, also set the AWS_PROFILE environment variable.
      // - An AWS Region must be specified either in the [default] profile
      //   or by setting the AWS_REGION environment variable.

      // Create an S3 client object.
      var s3Client = new AmazonS3Client();

      // Parse the command line arguments for the bucket name.
      if(GetBucketName(args, out String bucketName))
      {
        // If a bucket name was supplied, create the bucket.
        // Call the API method directly
        try
        {
          Console.WriteLine($"\nCreating bucket {bucketName}...");
          var createResponse = await s3Client.PutBucketAsync(bucketName);
          Console.WriteLine($"Result: {createResponse.HttpStatusCode.ToString()}");
        }
        catch (Exception e)
        {
          Console.WriteLine("Caught exception when creating a bucket:");
          Console.WriteLine(e.Message);
        }
      }

      // List the buckets owned by the user.
      // Call a class method that calls the API method.
      Console.WriteLine("\nGetting a list of your buckets...");
      var listResponse = await MyListBucketsAsync(s3Client);
      Console.WriteLine($"Number of buckets: {listResponse.Buckets.Count}");
      foreach(S3Bucket b in listResponse.Buckets)
      {
        Console.WriteLine(b.BucketName);
      }

      var request = new PutObjectRequest
      {
        BucketName = "screenshot-unity",
        Key = "calvin-upload-test",
        ContentBody = "deadbeaf"
      };
      var response = await s3Client.PutObjectAsync(request);

      /* create dynamoDB table */
      AmazonDynamoDBClient client = new AmazonDynamoDBClient();
      string tableName = "ProductCatalog";
/* can only create once

      var tbl_request = new CreateTableRequest
      {
        TableName = tableName,
        AttributeDefinitions = new List<AttributeDefinition>()
        {
          new AttributeDefinition
          {
            AttributeName = "Id",
            AttributeType = "N"
          }
        },
        KeySchema = new List<KeySchemaElement>()
        {
          new KeySchemaElement
          {
            AttributeName = "Id",
            KeyType = "HASH"  //Partition key
          }
        },
        ProvisionedThroughput = new ProvisionedThroughput
        {
          ReadCapacityUnits = 10,
          WriteCapacityUnits = 5
        }
      };
      var tbl_response = await client.CreateTableAsync(tbl_request);
*/

      /* put dynamoDB item */
      var item_request = new PutItemRequest
      {
         TableName = tableName,
         Item = new Dictionary<string, AttributeValue>()
            {
                { "Id", new AttributeValue { N = "202" }},
                { "Title", new AttributeValue { S = "Book 202 Title" }},
                { "ISBN", new AttributeValue { S = "11-11-11-11" }},
                { "Price", new AttributeValue { S = "20.00" }},
                {
                  "Authors",
                  new AttributeValue
                  { SS = new List<string>{"Author1", "Author2"}   }
                }
            }
      };
      var item_response = await client.PutItemAsync(item_request);

      /* update request */
      var update_request = new UpdateItemRequest
      {
          TableName = tableName,
          Key = new Dictionary<string,AttributeValue>() { { "Id", new AttributeValue { N = "201" } } },
          ExpressionAttributeNames = new Dictionary<string,string>()
          {
              {"#A", "Authors"},
          },
          ExpressionAttributeValues = new Dictionary<string, AttributeValue>()
          {
              {":auth",new AttributeValue { SS = {"Author YY","Author ZZ"}}},
          },

          // This expression does the following:
          // 1) Adds two new authors to the list
          // 2) Reduces the price
          // 3) Adds a new attribute to the item
          // 4) Removes the ISBN attribute from the item
          UpdateExpression = "ADD #A :auth"
      };
      var update_response = await client.UpdateItemAsync(update_request);

    }


    //
    // Method to parse the command line.
    private static Boolean GetBucketName(string[] args, out String bucketName)
    {
      Boolean retval = false;
      bucketName = String.Empty;
      if (args.Length == 0)
      {
        Console.WriteLine("\nNo arguments specified. Will simply list your Amazon S3 buckets." +
          "\nIf you wish to create a bucket, supply a valid, globally unique bucket name.");
        bucketName = String.Empty;
        retval = false;
      }
      else if (args.Length == 1)
      {
        bucketName = args[0];
        retval = true;
      }
      else
      {
        Console.WriteLine("\nToo many arguments specified." +
          "\n\ndotnet_tutorials - A utility to list your Amazon S3 buckets and optionally create a new one." +
          "\n\nUsage: S3CreateAndList [bucket_name]" +
          "\n - bucket_name: A valid, globally unique bucket name." +
          "\n - If bucket_name isn't supplied, this utility simply lists your buckets.");
        Environment.Exit(1);
      }
      return retval;
    }


    //
    // Async method to get a list of Amazon S3 buckets.
    private static async Task<ListBucketsResponse> MyListBucketsAsync(IAmazonS3 s3Client)
    {
      return await s3Client.ListBucketsAsync();
    }

  }
}
