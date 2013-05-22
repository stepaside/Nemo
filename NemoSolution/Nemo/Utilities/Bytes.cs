using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO.Compression;
using System.IO;
using System.Security.Cryptography;
using Nemo.Extensions;

namespace Nemo.Utilities
{
    public static class Bytes
    {
        public static byte[] Compress(byte[] buffer)
        {
            buffer.ThrowIfNull("buffer");

            var compressed = new MemoryStream();
            using (var gzip = new GZipStream(compressed, CompressionMode.Compress, true))
            {
                // Write the input to the memory stream via the ZIP stream.
                gzip.Write(buffer, 0, buffer.Length);
            }
            return compressed.ToArray();            
        }

        public static byte[] Encrypt(byte[] buffer, ICryptoTransform encryptor)
        {
            buffer.ThrowIfNull("buffer");
            encryptor.ThrowIfNull("encryptor");

            var originalSize = buffer.Length;
            var stream = new MemoryStream(BitConverter.GetBytes(originalSize).Concat(buffer).ToArray());

            // Encrypt the compressed memory stream into the encrypted memory stream.
            MemoryStream encrypted = new MemoryStream();
            using (var cryptor = new CryptoStream(encrypted, encryptor, CryptoStreamMode.Write))
            {
                // Write the stream to the encrypted memory stream.
                cryptor.Write(stream.ToArray(), 0, (int)stream.Length);
                cryptor.FlushFinalBlock();
            }

            return encrypted.ToArray();
        }

        public static byte[] Decompress(byte[] buffer)
        {
            buffer.ThrowIfNull("buffer");

            // Create the zip stream to decompress.
            using (var gzip = new GZipStream(new MemoryStream(buffer), CompressionMode.Decompress, false))
            {
                // Read all bytes in the zip stream and return them.
                buffer = ReadAllBytes(gzip);
            }
            return buffer;
        }

        public static byte[] Decrypt(byte[] buffer, ICryptoTransform decryptor)
        {
            buffer.ThrowIfNull("buffer");
            decryptor.ThrowIfNull("decryptor");

            // Create the array that holds the result.
            byte[] decrypted = new byte[buffer.Length];
            // Create the crypto stream that is used for decrypt. The first argument holds the input as memory stream.
            using (var cryptor = new CryptoStream(new MemoryStream(buffer), decryptor, CryptoStreamMode.Read))
            {
                // Read the encrypted values into the decrypted stream. Decrypts the content.
                cryptor.Read(decrypted, 0, decrypted.Length);
            }
            buffer = decrypted;

            int originalSize = BitConverter.ToInt32(buffer.Take(4).ToArray(), 0);
            buffer = buffer.Skip(4).Take(originalSize).ToArray();

            return buffer;
        }

        public static byte[] Random(int length)
        {
            var data = new byte[length];
            new RNGCryptoServiceProvider().GetBytes(data);
            return data;
        }

        public static string ToHex(byte[] bytes)
        {
            char[] c = new char[bytes.Length * 2];
            int b;
            for (int i = 0; i < bytes.Length; i++)
            {
                b = bytes[i] >> 4;
                c[i * 2] = (char)(55 + b + (((b - 10) >> 31) & -7));
                b = bytes[i] & 0xF;
                c[i * 2 + 1] = (char)(55 + b + (((b - 10) >> 31) & -7));
            }
            return new string(c);
        }

        public static string ToString(byte[] bytes, Encoding encoding = null)
        {
            if (encoding == null)
            {
                encoding = Encoding.UTF8;
            }
            return encoding.GetString(bytes);
        }

        /// <summary>
        /// Reads all bytes in the given zip stream and returns them.
        /// </summary>
        /// <param name="gzip">The zip stream that is processed.</param>
        /// <returns></returns>
        private static byte[] ReadAllBytes(GZipStream zip)
        {
            zip.ThrowIfNull("zip");

            int buffersize = 100;
            byte[] buffer = new byte[buffersize];
            int offset = 0, read = 0, size = 0;
            do
            {
                // If the buffer doesn't offer enough space left create a new array
                // with the double size. Copy the current buffer content to that array
                // and use that as new buffer.
                if (buffer.Length < size + buffersize)
                {
                    byte[] tmp = new byte[buffer.Length * 2];
                    Array.Copy(buffer, tmp, buffer.Length);
                    buffer = tmp;
                }

                // Read the net chunk of data.
                read = zip.Read(buffer, offset, buffersize);

                // Increment offset and read size.
                offset += buffersize;
                size += read;
            } while (read == buffersize); // Terminate if we read less then the buffer size.

            // Copy only that amount of data to the result that has actually been read!
            byte[] result = new byte[size];
            Array.Copy(buffer, result, size);
            return result;
        }
    }
}
