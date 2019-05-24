/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.client

import com.mongodb.AutoEncryptionOptions
import com.mongodb.KeyVaultEncryptionOptions
import com.mongodb.MongoCommandException
import com.mongodb.MongoNamespace
import com.mongodb.client.model.vault.DataKeyOptions
import com.mongodb.client.model.vault.EncryptOptions
import com.mongodb.client.vault.KeyVaults
import com.mongodb.crypt.capi.MongoCryptException
import org.bson.BsonArray
import org.bson.BsonBinary
import org.bson.BsonBinarySubType
import org.bson.BsonBoolean
import org.bson.BsonDecimal128
import org.bson.BsonDocument
import org.bson.BsonDouble
import org.bson.BsonInt32
import org.bson.BsonInt64
import org.bson.BsonNull
import org.bson.BsonObjectId
import org.bson.BsonString
import org.bson.BsonSymbol
import org.bson.BsonTimestamp
import org.bson.types.Decimal128
import org.bson.types.ObjectId

import static com.mongodb.client.Fixture.getDefaultDatabaseName
import static com.mongodb.client.Fixture.getMongoClient
import static com.mongodb.client.Fixture.getMongoClientSettings
import static com.mongodb.client.Fixture.getMongoClientSettingsBuilder
import static java.util.Collections.emptyMap
import static java.util.Collections.singletonMap
import static org.junit.Assume.assumeTrue

class ClientSideEncryptionProseTestSpecification extends FunctionalSpecification {

    private final MongoNamespace keyVaultNamespace = new MongoNamespace('test.datakeys')
    private final MongoCollection dataKeyCollection = getMongoClient()
            .getDatabase(keyVaultNamespace.databaseName).getCollection(keyVaultNamespace.collectionName, BsonDocument)
    private final Map<String, Map<String, ? extends Object>> localProviderProperties =
            ['local': ['key': new byte[96]]]
    private final Map<String, Map<String, ? extends Object>> awsProviderProperties =
            ['aws': ['accessKeyId'    : System.getProperty('org.mongodb.test.awsAccessKeyId'),
                     'secretAccessKey': System.getProperty('org.mongodb.test.awsSecretAccessKey')]]
    private final BsonDocument awsMasterKey = new BsonDocument('region', new BsonString('us-east-1'))
            .append('key', new BsonString('arn:aws:kms:us-east-1:579766882180:key/89fcc2c4-08b0-4bd9-9f25-e30687b580d0'))
    private final MongoNamespace autoEncryptingCollectionNamespace = new MongoNamespace(getDefaultDatabaseName(),
            'ClientSideEncryptionProseTestSpecification')
    private final MongoClient autoEncryptingClient =
            MongoClients.create(getMongoClientSettingsBuilder()
                    .autoEncryptionOptions(new AutoEncryptionOptions(null, keyVaultNamespace.fullName,
                    localProviderProperties, singletonMap(autoEncryptingCollectionNamespace.fullName,
                    BsonDocument.parse(
'''
    {
        "properties": {
            "random": {
                "encrypt": {
                    "keyId": [
                        {
                            "$binary": {
                                "base64": "YWFhYWFhYWFhYWFhYWFhYQ==",
                                "subType": "04"
                            }
                        }
                    ],
                    "bsonType": "string",
                    "algorithm": "AEAD_AES_256_CBC_HMAC_SHA_512-Random"
                }
            }
        },
        "bsonType": "object"
    }'''
                    )), emptyMap(), false)).build())

    private final BsonDocument localDataKeyDocument = BsonDocument.parse(
            '''
{
  "_id": {
    "$binary": {
        "base64": "YWFhYWFhYWFhYWFhYWFhYQ==",
        "subType": "04"
    }
},
  "keyMaterial": {
    "$binary": {
      "base64": "db27rshiqK4Jqhb2xnwK4RfdFb9JuKeUe6xt5aYQF4o62tS75b7B4wxVN499gND9UVLUbpVKoyUoaZAeA895OENP335b8n8OwchcTFqS44t+P3zmhteYUQLIWQXaIgon7gEgLeJbaDHmSXS6/7NbfDDFlB37N7BP/2hx1yCOTN6NG/8M1ppw3LYT3CfP6EfXVEttDYtPbJpbb7nBVlxD7w==",
      "subType": "00"
    }
  },
  "creationDate": { "$date": { "$numberLong": "1232739599082000" } },
  "updateDate": { "$date": { "$numberLong": "1232739599082000" } },
  "status": { "$numberInt": "0" },
  "masterKey": { "provider": "local" }
}
''')

    private final BsonDocument awsDataKeyDocument = BsonDocument.parse(
            '''
{
    "_id": {
        "$binary": {
            "base64": "AAAAAAAAAAAAAAAAAAAAAA==",
            "subType": "04"
        }
    },
    "version": {
        "$numberLong": "0"
    },
    "keyAltNames": [ "altname1", "altname2" ],
    "keyMaterial": {
        "$binary": {
            "base64": "AQICAHhQNmWG2CzOm1dq3kWLM+iDUZhEqnhJwH9wZVpuZ94A8gG2Qei6UQdbOR5RWhPSrNwnAAAAwjCBvwYJKoZIhvcNAQcGoIGxMIGuAgEAMIGoBgkqhkiG9w0BBwEwHgYJYIZIAWUDBAEuMBEEDMJ2xcv8wKZzqTIX/gIBEIB7dNUvInthJHEd55QaEyVYSacoPvMlx2wzhxW+E6MBcfP+nCrzByLkqyHRhWs5NEvrOBT2nuc87iZIuK/WNR/pl5eK1xQ/8Cy0GrMfD3GIjYDlZ6aWc06cJvwvZd3Cgqx0pQnunwNr2EfStTGj7gHW23kzkfpxDiphqPnH",
            "subType": "00"
        }
    },
    "creationDate": {
        "$date": {
            "$numberLong": "1553026537755"
        }
    },
    "updateDate": {
        "$date": {
            "$numberLong": "1553026537755"
        }
    },
    "status": {
        "$numberInt": "1"
    },
    "masterKey": {
        "key": "arn:aws:kms:us-east-1:579766882180:key/89fcc2c4-08b0-4bd9-9f25-e30687b580d0",
        "region": "us-east-1",
        "provider": "aws"
    }
}'''
    )

    def setup() {
        assumeTrue('Key vault tests disabled',
                System.getProperty('org.mongodb.test.awsAccessKeyId') != null
                        && !System.getProperty('org.mongodb.test.awsAccessKeyId').isEmpty())
        dataKeyCollection.drop()
    }

    def 'should create local data key'() {
        given:
        def keyVault = KeyVaults.create(new KeyVaultEncryptionOptions(getMongoClientSettings(), keyVaultNamespace.fullName,
                localProviderProperties))

        when:
        def id = keyVault.createDataKey('local')

        then:
        id != null
        id.type == BsonBinarySubType.UUID_STANDARD.value
        dataKeyCollection.find().first().getBinary('_id') == id
    }

    def 'should create aws data key'() {
        given:
        def keyVault = KeyVaults.create(new KeyVaultEncryptionOptions(getMongoClientSettings(), keyVaultNamespace.fullName,
                awsProviderProperties))

        when:
        def id = keyVault.createDataKey('aws', new DataKeyOptions().masterKey(awsMasterKey))

        then:
        id != null
        id.type == BsonBinarySubType.UUID_STANDARD.value
        dataKeyCollection.find().first().getBinary('_id') == id
    }

    def 'should explicitly encrypt and decrypt with local provider'() {
        given:
        dataKeyCollection.insertOne(localDataKeyDocument)
        def keyVault = KeyVaults.create(new KeyVaultEncryptionOptions(getMongoClientSettings(), keyVaultNamespace.fullName,
                localProviderProperties))
        def value = new BsonString('hello')

        when:
        def encryptedValue = keyVault.encrypt(value, new EncryptOptions('AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic')
                .keyId(localDataKeyDocument.getBinary('_id')))

        then:
        encryptedValue == new BsonBinary((byte) 6,
                Base64.decoder.decode('AWFhYWFhYWFhYWFhYWFhYWEC7ubnsHvOUXvbE4406+XawIhcl+fsvNWO7moBSY7ABkPuCTzsitrqWWp1FbaaT05muIESiB8daggJPgwarTQ3cQ=='))

        when:
        def decryptedValue = keyVault.decrypt(encryptedValue)

        then:
        decryptedValue == value
    }

    def 'should explicitly encrypt and decrypt with aws provider'() {
        given:
        dataKeyCollection.insertOne(awsDataKeyDocument)
        def keyVault = KeyVaults.create(new KeyVaultEncryptionOptions(getMongoClientSettings(), keyVaultNamespace.fullName,
                awsProviderProperties))
        def value = new BsonString('hello')

        when:
        def encryptedValue = keyVault.encrypt(value, new EncryptOptions('AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic')
                .keyId(awsDataKeyDocument.getBinary('_id')))

        then:
        encryptedValue == new BsonBinary((byte) 6,
                Base64.decoder.decode('AQAAAAAAAAAAAAAAAAAAAAACN0NwWlfe6YPGDEw+ObxEzbEtk45ewF3sIH2Oj7F0xd3GYoxCGCIp9gg0Q1uHTwdVWwG58SFhJyo4305IVoikEQ=='))

        when:
        def decryptedValue = keyVault.decrypt(encryptedValue)

        then:
        decryptedValue == value
    }

    /*
Test explicit encrypt of invalid values.

- Create a `KeyVault` with either a "local" or "aws" KMS provider
- Use `KeyVault.encrypt` to attempt to encrypt each BSON type with deterministic encryption.

  - Expect `document` and `array` to fail. An exception MUST be thrown.
  - Expect a BSON binary subtype 6 to fail. An exception MUST be thrown.
  - Expect all other values to succeed.

- Use `KeyVault.encrypt` to attempt to encrypt a document using randomized encryption.

  - Expect a BSON binary subtype 6 to fail. An exception MUST be thrown.
  - Expect all other values to succeed.

 */

    def 'should encrypt valid values'() {
        given:
        dataKeyCollection.insertOne(localDataKeyDocument)
        def keyVault = KeyVaults.create(new KeyVaultEncryptionOptions(getMongoClientSettings(), keyVaultNamespace.fullName,
                localProviderProperties))
        def options = new EncryptOptions(algorithm).keyId(localDataKeyDocument.getBinary('_id'))

        when:
        keyVault.encrypt(value, options)

        then:
        noExceptionThrown()

        where:
        [value, algorithm] << [
                [BsonBoolean.TRUE,
                 new BsonInt32(1),
                 new BsonInt64(1L),
                 new BsonTimestamp(42),
                 new BsonObjectId(new ObjectId()),
                 new BsonBinary(new byte[4]),
                 new BsonString('42'),
                 new BsonSymbol('42'),
                 BsonNull.VALUE],
                ['AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic',
                 'AEAD_AES_256_CBC_HMAC_SHA_512-Random']
        ].combinations()
    }

    def 'should fail to encrypt invalid values with deterministic encryption'() {
        given:
        dataKeyCollection.insertOne(localDataKeyDocument)
        def keyVault = KeyVaults.create(new KeyVaultEncryptionOptions(getMongoClientSettings(), keyVaultNamespace.fullName,
                localProviderProperties))
        def options = new EncryptOptions('AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic')
                .keyId(localDataKeyDocument.getBinary('_id'))

        when:
        keyVault.encrypt(new BsonDouble(1.0), options)

        then:
        thrown(MongoCryptException)

        when:
        keyVault.encrypt(new BsonDecimal128(new Decimal128('1.0')), options)

        then:
        thrown(MongoCryptException)

        when:
        keyVault.encrypt(new BsonBinary((byte) 6,
                Base64.decoder.decode('AQAAAAAAAAAAAAAAAAAAAAACN0NwWlfe6YPGDEw+ObxEzbEtk45ewF3sIH2Oj7F0xd3GYoxCGCIp9gg0Q1uHTwdVWwG58SFhJyo4305IVoikEQ==')),
                options)

        then:
        thrown(MongoCryptException)

        when:
        keyVault.encrypt(new BsonArray([new BsonInt32(1), new BsonInt32(1)]), options)

        then:
        thrown(MongoCryptException)

        when:
        keyVault.encrypt(new BsonDocument(), options)

        then:
        thrown(MongoCryptException)
    }

    def 'should fail to encrypt invalid values with randomized encryption'() {
        given:
        dataKeyCollection.insertOne(localDataKeyDocument)
        def keyVault = KeyVaults.create(new KeyVaultEncryptionOptions(getMongoClientSettings(), keyVaultNamespace.fullName,
                localProviderProperties))
        def options = new EncryptOptions('AEAD_AES_256_CBC_HMAC_SHA_512-Randomized')
                .keyId(localDataKeyDocument.getBinary('_id'))

        when:
        keyVault.encrypt(new BsonBinary((byte) 6,
                Base64.decoder.decode('AQAAAAAAAAAAAAAAAAAAAAACN0NwWlfe6YPGDEw+ObxEzbEtk45ewF3sIH2Oj7F0xd3GYoxCGCIp9gg0Q1uHTwdVWwG58SFhJyo4305IVoikEQ==')),
                options)

        then:
        thrown(MongoCryptException)
    }

    def 'should encrypt values with randomized encryption that are invalid with deterministic encryption'() {
        given:
        dataKeyCollection.insertOne(localDataKeyDocument)
        def keyVault = KeyVaults.create(new KeyVaultEncryptionOptions(getMongoClientSettings(), keyVaultNamespace.fullName,
                localProviderProperties))
        def options = new EncryptOptions('AEAD_AES_256_CBC_HMAC_SHA_512-Random')
                .keyId(localDataKeyDocument.getBinary('_id'))

        when:
        keyVault.encrypt(new BsonDouble(1.0), options)

        then:
        noExceptionThrown()

        when:
        keyVault.encrypt(new BsonDecimal128(Decimal128.parse('1.0')), options)

        then:
        noExceptionThrown()

        when:
        keyVault.encrypt(new BsonArray([new BsonInt32(1), new BsonInt32(1)]), options)

        then:
        noExceptionThrown()

        when:
        keyVault.encrypt(new BsonDocument(), options)

        then:
        noExceptionThrown()
    }

    /*
    Test explicit encryption with auto decryption.

     - Create a `KeyVault` with either a "local" or "aws" KMS provider
     - Use `KeyVault.encrypt` to encrypt a value.
     - Create a document, setting some field to the value.
     - Insert the document into a collection.
     - Find the document. Verify both the value matches the originally set value.
     */

    def 'should auto decrypt explicitly encrypted value'() {
        given:
        dataKeyCollection.insertOne(localDataKeyDocument)
        def keyVault = KeyVaults.create(new KeyVaultEncryptionOptions(getMongoClientSettings(), keyVaultNamespace.fullName,
                localProviderProperties))
        def options = new EncryptOptions('AEAD_AES_256_CBC_HMAC_SHA_512-Random')
                .keyId(localDataKeyDocument.getBinary('_id'))
        def unencryptedValue = new BsonString('super secret')
        def autoEncryptingCollection = autoEncryptingClient.getDatabase(getDefaultDatabaseName())
                .getCollection(getCollectionName(), BsonDocument)


        when:
        def encryptedValue = keyVault.encrypt(unencryptedValue, options)
        autoEncryptingCollection.insertOne(new BsonDocument('_id', encryptedValue))
        def decryptedValue = autoEncryptingCollection.find().first().getString('_id')

        then:
        unencryptedValue == decryptedValue
    }

    /*
    Test explicit encrypting an auto encrypted field.

     - Create a `KeyVault` with either a "local" or "aws" KMS provider
     - Create a collection with a JSONSchema specifying an encrypted field.
     - Use `KeyVault.encrypt` to encrypt a value.
     - Create a document, setting the auto-encrypted field to the value.
     - Insert the document. Verify an exception is thrown.
     */

    def 'should throw when inserting a document with an explicitly encrypted value that should be auto-encrypted'() {
        dataKeyCollection.insertOne(localDataKeyDocument)
        def keyVault = KeyVaults.create(new KeyVaultEncryptionOptions(getMongoClientSettings(), keyVaultNamespace.fullName,
                localProviderProperties))
        def options = new EncryptOptions('AEAD_AES_256_CBC_HMAC_SHA_512-Random')
                .keyId(localDataKeyDocument.getBinary('_id'))
        def unencryptedValue = new BsonString('super secret')
        def autoEncryptingCollection = autoEncryptingClient.getDatabase(autoEncryptingCollectionNamespace.databaseName)
                .getCollection(autoEncryptingCollectionNamespace.collectionName, BsonDocument)

        when:
        def encryptedValue = keyVault.encrypt(unencryptedValue, options)
        autoEncryptingCollection.insertOne(new BsonDocument('random', encryptedValue))

        then:
        thrown(MongoCommandException)
    }

    /*
    Test deduplication of concurrent requests.

     - Create a `KeyVault` with either a "local" or "aws" KMS provider
     - Create a collection with a JSONSchema specifying an encrypted field.
     - Enable the following failpoint:

        db.adminCommand({configureFailPoint: "hangBeforeListCollections", mode: "alwaysOn" })

     - Attempt to insert into the collection, setting the encrypted field. Synchronous drivers MUST do this in a separate thread. The
       operation will hang while waiting for a response from listCollections.
     - Attempt another insert, setting the same encrypted field. Synchronous drivers MUST do this in a separate thread. The operation will
       hang while waiting on the first insert operation.
     - Disable the failpoint:

       db.adminCommand({configureFailPoint: "hangBeforeListCollections", mode: "off" })

     - Verify both insert operations succeed. Verify that exactly one listCollections command is run on the collection, and that exactly
    one find is run on the datakeys collection.
     */
    def 'should decuplicate concurrent requests'() {
        // TODO: implement this test
        assumeTrue(false)
    }
}
