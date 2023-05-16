package com.amazon.ion

import com.amazon.ion.system.IonSystemBuilder
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.math.BigInteger

class IonExtensionsTest {

    val ION = IonSystemBuilder.standard().build()

    @Test
    fun `asIterator() should iterate over child elements`() {
        val ionValue = ION.singleValue("[1, a, false]")
        val itr = ionValue.asIterator()
        assertTrue(itr.hasNext())
        assertEquals(BigInteger.ONE, itr.next().bigIntValueOrNull())
        assertTrue(itr.hasNext())
        assertEquals("a", itr.next().stringValueOrNull())
        assertTrue(itr.hasNext())
        assertEquals(false, itr.next().boolValueOrNull())
        assertFalse(itr.hasNext())
        assertThrows<NoSuchElementException> { itr.next() }
    }

    @Test
    fun `asIterator() should return an iterator over self if this is not a container`() {
        val ionValue = ION.singleValue("a")
        val itr = ionValue.asIterator()
        assertTrue(itr.hasNext())
        assertEquals("a", itr.next().stringValueOrNull())
        assertFalse(itr.hasNext())
        assertThrows<NoSuchElementException> { itr.next() }
    }
}