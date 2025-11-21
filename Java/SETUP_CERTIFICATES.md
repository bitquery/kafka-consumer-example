# Setting up Certificates


#### **Step 1: Convert PEM to PKCS12 Format**
- Create the PKCS12 file using `openssl`:
  ```bash
  openssl pkcs12 -export -in client.cer.pem -inkey client.key.pem -out client.p12 -name mykey -CAfile ca.cer.pem -caname root -password pass:123456
  ```
  - `-password pass:123456`: Sets the password for the PKCS12 file.

#### **Step 2: Convert PKCS12 to JKS Format**
- Convert the PKCS12 file to JKS format using `keytool`:
  ```bash
  keytool -importkeystore -deststorepass 123456 -destkeypass 123456 -destkeystore keystore.jks -srckeystore client.p12 -srcstoretype PKCS12 -srcstorepass 123456 -alias mykey
  ```
  - `-deststorepass 123456`: Sets the password for the JKS keystore.
  - `-destkeypass 123456`: Sets the password for the key in the JKS keystore.
  - `-srcstorepass 123456`: Uses `123456` as the password for the PKCS12 file.

#### **Step 3: Update Kafka Properties**
- Update Kafka properties to use the JKS keystore:
  ```java
  props.put("ssl.keystore.location", "certs/keystore.jks");
  props.put("ssl.keystore.password", "123456");
  props.put("ssl.key.password", "123456");
  ```

---

### Truststore Configuration

#### **Step 1: Create a Truststore Containing the Broker's CA Certificate**

1. **Retrieve the Broker's Certificate**
   ```bash
   echo | openssl s_client -connect rpk0.bitquery.io:9093 -servername rpk0.bitquery.io -showcerts > broker_certs.pem
   ```

2. **Split the Certificates**
   - Extract and save individual certificates (e.g., `broker_cert.pem`, `intermediate_cert.pem`).

3. **Import Certificates into Truststore**
   ```bash
   keytool -import -alias brokerCert -file broker_certs.pem -keystore clienttruststore.jks -storepass truststorepassword
   ```
