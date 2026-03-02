declare module 'sse4_crc32' {
  interface CRC32C {
    calculate(data: Buffer | string, initial?: number): number;
  }
  const crc32c: CRC32C;
  export default crc32c;
}
