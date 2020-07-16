package company.evo.persistent.hashmap

import company.evo.persistent.hashmap.keyTypes.Int.*
import company.evo.persistent.hashmap.valueTypes.Float.*
import company.evo.processor.KeyValueTemplate


interface PersistentHashMapRO_Int_Float : PersistentHashMapRO {
    fun contains(key: K): Boolean
    fun get(key: K, defaultValue: V): V
    fun tombstones(): Int

    fun dump(dumpContent: Boolean): String
}

interface PersistentHashMapIterator_Int_Float {
    fun next(): Boolean
    fun key(): K
    fun value(): V
}

@KeyValueTemplate(
    keyTypes = ["Int", "Long"],
    valueTypes = ["Short", "Int", "Long", "Double", "Float"]
)
interface PersistentHashMap_Int_Float : PersistentHashMapRO_Int_Float, PersistentHashMap {
    fun put(key: K, value: V): PutResult
    fun remove(key: K): Boolean
    fun flush()
    fun iterator(): PersistentHashMapIterator_Int_Float
}
