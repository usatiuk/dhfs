package com.usatiuk.dhfs.peertrust;

import org.apache.commons.codec.digest.DigestUtils;
import org.bouncycastle.asn1.ASN1ObjectIdentifier;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.BasicConstraints;
import org.bouncycastle.cert.CertIOException;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.math.BigInteger;
import java.security.*;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.Calendar;
import java.util.Date;

/**
 * Helper class for generating and manipulating X.509 certificates.
 */
public class CertificateTools {

    /**
     * Converts a byte array to an X.509 certificate.
     *
     * @param bytes the byte array representing the certificate
     * @return the X.509 certificate
     */
    public static X509Certificate certFromBytes(byte[] bytes) {
        try {
            CertificateFactory certFactory = CertificateFactory.getInstance("X.509");
            InputStream in = new ByteArrayInputStream(bytes);
            return (X509Certificate) certFactory.generateCertificate(in);
        } catch (CertificateException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Generates a random RSA key pair.
     * @return the generated RSA key pair
     */
    public static KeyPair generateKeyPair() {
        try {
            KeyPairGenerator keyGen = KeyPairGenerator.getInstance("RSA");
            keyGen.initialize(2048); //FIXME:
            return keyGen.generateKeyPair();
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Generates an X.509 certificate using the provided key pair and subject name.
     *
     * @param keyPair the key pair to use for the certificate
     * @param subject the subject name for the certificate
     * @return the generated X.509 certificate
     */
    public static X509Certificate generateCertificate(KeyPair keyPair, String subject) {
        try {
            Provider bcProvider = new BouncyCastleProvider();
            Security.addProvider(bcProvider);

            Date startDate = new Date();

            X500Name cnName = new X500Name("CN=" + subject);
            BigInteger certSerialNumber = new BigInteger(DigestUtils.sha256(subject));

            Calendar calendar = Calendar.getInstance();
            calendar.setTime(startDate);
            calendar.add(Calendar.YEAR, 999);

            Date endDate = calendar.getTime();

            ContentSigner contentSigner = new JcaContentSignerBuilder("SHA256WithRSA").build(keyPair.getPrivate());

            JcaX509v3CertificateBuilder certBuilder = new JcaX509v3CertificateBuilder(cnName, certSerialNumber, startDate, endDate, cnName, keyPair.getPublic());

            BasicConstraints basicConstraints = new BasicConstraints(false);
            certBuilder.addExtension(new ASN1ObjectIdentifier("2.5.29.19"), true, basicConstraints);

            return new JcaX509CertificateConverter().setProvider(bcProvider).getCertificate(certBuilder.build(contentSigner));
        } catch (OperatorCreationException | CertificateException | CertIOException e) {
            throw new RuntimeException(e);
        }
    }
}
