# Crypto Transform Module

Crypto transform module encrypts or decrypts the value of a specified field value.

## Transform module common parameters

| parameter | optional | type | description |
| --- | --- | --- | --- |
| name | required | String | Step name. specified to be unique in config file. |
| module | required | String | Specified `crypto` |
| inputs | required | Array<String\> | Specify the names of the step from which you want to process the data, including the name of the transform. |
| parameters | required | Map<String,Object\> | Specify the following individual parameters. |

## Crypto transform module parameters

| parameter | optional | type | description |
| --- | --- | --- | --- |
| mode | required | String | Select whether to `encrypt` or `decrypt`. Currently, only `decrypt` is supported. |
| fields | required | Array<String\> | Specify the names of the field you want to encrypt or decrypt. The type of the field specified here must be a byte array. |
| algorithm | optional | String | Specifies the algorithm for encryption and decryption. Currently only `AES256` is supported. |
| failFast | optional | Boolean | Specify whether the job should fail immediately if there are records that have failed to decrypt or encrypt. Default is True. |
| keyProvider | required | KeyProvider | Specify the method of providing the key. In addition to the key itself, it also specifies the storage service where the key is stored. |
| keyDecryptor | optional | KeyDecryptor | Specifies how to decrypt a key if the key itself is encrypted. |
| keyExtractor | optional | KeyExtractor | Specifies how to retrieve the key from the provided key information. (Used when the keyProvider provides information in JSON format that includes information other than the encryption key) |
| vault | selective required | VaultSetting | Specify basic vault configuration information when using Hashicorp's Vault service for key storage and key decryption. |

## KeyProvider parameters

This setting specifies the method of providing the encryption key information.

| parameter | optional | type | description |
| --- | --- | --- | --- |
| base64text | selective required | String | A Base64 encoded string of the encryption key. You can also specify an encrypted cryptographic key and use keyDecryptor to decrypt it. |
| vault.kvPath | selective required | String | Specify the path to retrieve the secret data when using the [Vault KV Secrets Engine](https://www.vaultproject.io/docs/secrets/kv/kv-v2) provided by Hashicorp as the key storage. |

## KeyDecryptor parameters

This setting is for decrypting the encryption key when it is encrypted.

| parameter | optional | type | description |
| --- | --- | --- | --- |
| jsonPath | optional | String | When the cryptographic key information provided by KeyProvider is in JSON format and you want to decrypt only specific values, specify the JSON Path that indicates the location. |
| vault.transitPath | required | String | Specify the transit path if you want to use the [Vault Transit Secrets Engine](https://www.vaultproject.io/api-docs/secret/transit) provided by Hashicorp to decrypt the encryption key. |

## KeyExtractor parameters

This setting is for extracting the encryption key when the encryption key information is saved in JSON format that contains strings other than the encryption key.

| parameter | optional | type | description |
| --- | --- | --- | --- |
| jsonPath | selective required | String | When cryptographic key information is provided in JSON format, specify the JSON Path when you want to use the value of a specific field as the cryptographic key. |
| template | selective required | String | Specifies the path of the GCS where the template file is placed when the logic for extracting the encryption key from the encryption key information is written in [Template Engine](https://freemarker.apache.org/). |

※ The cryptographic key extracted as a string is finally converted into a byte array by Base64 decoding and used.

## VaultSetting parameters

Specify the basic configuration information for Vault when using Vault services to store and decrypt encryption key information.

| parameter | optional | type | description |
| --- | --- | --- | --- |
| host | required | String | Specifies the hostname with port number of the Vault server. |
| role | required | String | Specify the Role for sending requests to the Vault server. |
| namespace | optional | String | Specify this if the vault server uses namespace. |

※ Access to the Vault server behaves as a Dataflow Worker service account.

## Related example config files

* [Spanner Decrypt to Cloud Storage(Avro)](../../../../examples/spanner-to-decrypt-to-avro.json)
