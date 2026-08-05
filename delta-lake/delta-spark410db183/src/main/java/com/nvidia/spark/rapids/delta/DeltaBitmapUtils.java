/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nvidia.spark.rapids.delta;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import org.roaringbitmap.RoaringBitmap;

final class DeltaBitmapUtils {
  private static final int DELTA_BITMAP_MAGIC_NUMBER_BYTE_SIZE = 4;
  private static final int NATIVE_MAGIC_NUMBER = 1681511376;
  private static final int PORTABLE_MAGIC_NUMBER = 1681511377;

  private DeltaBitmapUtils() {
  }

  static int portableMagicNumber() {
    return PORTABLE_MAGIC_NUMBER;
  }

  static int nativeMagicNumber() {
    return NATIVE_MAGIC_NUMBER;
  }

  static RoaringBitmap[] deserializeDeltaBitmap(byte[] bytes) throws IOException {
    ByteBuffer buffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
    ensureRemaining(buffer, DELTA_BITMAP_MAGIC_NUMBER_BYTE_SIZE);
    int magicNumber = buffer.getInt();
    if (magicNumber == PORTABLE_MAGIC_NUMBER) {
      return deserializePortable(buffer);
    } else if (magicNumber == NATIVE_MAGIC_NUMBER) {
      return deserializeNative(buffer);
    }
    throw new IOException("Unexpected RoaringBitmapArray magic number " + magicNumber);
  }

  static RoaringBitmap[] deserializeNative(ByteBuffer buffer) throws IOException {
    ensureRemaining(buffer, Integer.BYTES);
    int numberOfBitmaps = buffer.getInt();
    if (numberOfBitmaps < 0) {
      throw new IOException("Invalid RoaringBitmapArray length " + numberOfBitmaps);
    }
    RoaringBitmap[] bitmaps = new RoaringBitmap[numberOfBitmaps];
    for (int index = 0; index < numberOfBitmaps; index++) {
      ensureRemaining(buffer, Integer.BYTES);
      int bitmapSize = buffer.getInt();
      if (bitmapSize < 0) {
        throw new IOException("Invalid serialized RoaringBitmap size " + bitmapSize);
      }
      ensureRemaining(buffer, bitmapSize);
      int startPosition = buffer.position();
      RoaringBitmap bitmap = new RoaringBitmap();
      bitmap.deserialize(buffer);
      if (bitmap.serializedSizeInBytes() != bitmapSize) {
        throw new IOException(
            "Serialized RoaringBitmap size does not match its native length prefix");
      }
      // RoaringBitmap.deserialize does not advance the supplied ByteBuffer.
      buffer.position(startPosition + bitmapSize);
      bitmaps[index] = bitmap;
    }
    return bitmaps;
  }

  private static RoaringBitmap[] deserializePortable(ByteBuffer buffer) throws IOException {
    ensureRemaining(buffer, Long.BYTES);
    long numberOfBitmaps = buffer.getLong();
    if (numberOfBitmaps < 0 || numberOfBitmaps > Integer.MAX_VALUE) {
      throw new IOException("Invalid RoaringBitmapArray length " + numberOfBitmaps);
    }

    List<RoaringBitmap> bitmaps = new ArrayList<>();
    int lastIndex = 0;
    for (long index = 0; index < numberOfBitmaps; index++) {
      ensureRemaining(buffer, Integer.BYTES);
      int key = buffer.getInt();
      if (key < lastIndex || key == Integer.MAX_VALUE) {
        throw new IOException("Invalid RoaringBitmapArray key " + key);
      }
      while (lastIndex < key) {
        bitmaps.add(new RoaringBitmap());
        lastIndex++;
      }

      int startPosition = buffer.position();
      RoaringBitmap bitmap = new RoaringBitmap();
      bitmap.deserialize(buffer);
      long bitmapSize = bitmap.serializedSizeInBytes();
      if (bitmapSize > Integer.MAX_VALUE) {
        throw new IOException("Serialized RoaringBitmap is too large: " + bitmapSize);
      }
      ensureRemaining(buffer, (int) bitmapSize);
      // RoaringBitmap.deserialize does not advance the supplied ByteBuffer.
      buffer.position(startPosition + (int) bitmapSize);
      bitmaps.add(bitmap);
      lastIndex++;
    }
    return bitmaps.toArray(new RoaringBitmap[0]);
  }

  static byte[] serializeAsDeltaPortable(RoaringBitmap[] bitmaps) {
    long serializedSize = DELTA_BITMAP_MAGIC_NUMBER_BYTE_SIZE + Long.BYTES;
    for (RoaringBitmap bitmap : bitmaps) {
      serializedSize += Integer.BYTES + bitmap.serializedSizeInBytes();
    }
    if (serializedSize > Integer.MAX_VALUE) {
      throw new IllegalStateException(
          "Serialized deletion vector bitmap is too large: " + serializedSize + " bytes");
    }
    // Roaring bitmap serialization stores all words in little-endian order by
    // format contract, independent of the native CPU byte order.
    // See https://github.com/RoaringBitmap/RoaringFormatSpec#general-layout
    ByteBuffer buffer = ByteBuffer.allocate((int) serializedSize).order(ByteOrder.LITTLE_ENDIAN);
    buffer.putInt(PORTABLE_MAGIC_NUMBER);
    buffer.putLong(bitmaps.length);
    for (int index = 0; index < bitmaps.length; index++) {
      buffer.putInt(index);
      bitmaps[index].serialize(buffer);
    }
    return buffer.array();
  }

  private static void ensureRemaining(ByteBuffer buffer, int required) throws IOException {
    if (required < 0 || buffer.remaining() < required) {
      throw new IOException(
          "Truncated RoaringBitmapArray: required " + required +
              " bytes but only " + buffer.remaining() + " remain");
    }
  }
}
