rm -f app/src/main/java/com/example/storecarrymesh/MeshModels.kt \
      app/src/main/java/com/example/storecarrymesh/MeshCore.kt \
      app/src/main/java/com/example/storecarrymesh/MeshQrAndCrypto.kt \
      app/src/main/java/com/example/storecarrymesh/WifiMeshTransport.kt \
      app/src/main/java/com/example/storecarrymesh/QrScannerDialog.kt \
      app/src/main/java/com/example/storecarrymesh/MainActivity.kt

cat << 'EOF' > app/src/main/java/com/example/storecarrymesh/AllInOneMesh.kt
package com.example.storecarrymesh

import android.Manifest
import android.annotation.SuppressLint
import android.app.AlertDialog
import android.app.Dialog
import android.bluetooth.BluetoothAdapter
import android.bluetooth.BluetoothDevice
import android.bluetooth.BluetoothManager
import android.bluetooth.BluetoothServerSocket
import android.bluetooth.BluetoothSocket
import android.content.Context
import android.content.pm.PackageManager
import android.graphics.Bitmap
import android.graphics.Color
import android.net.nsd.NsdManager
import android.net.nsd.NsdServiceInfo
import android.os.Build
import android.os.Bundle
import android.text.InputType
import android.util.Base64
import android.util.Size
import android.widget.Button
import android.widget.EditText
import android.widget.ImageView
import android.widget.LinearLayout
import android.widget.ScrollView
import android.widget.TextView
import androidx.activity.ComponentActivity
import androidx.activity.result.contract.ActivityResultContracts
import androidx.annotation.OptIn
import androidx.camera.core.CameraSelector
import androidx.camera.core.ExperimentalGetImage
import androidx.camera.core.ImageAnalysis
import androidx.camera.core.ImageProxy
import androidx.camera.core.Preview
import androidx.camera.lifecycle.ProcessCameraProvider
import androidx.camera.view.PreviewView
import androidx.core.content.ContextCompat
import androidx.lifecycle.Lifecycle
import androidx.lifecycle.LifecycleOwner
import androidx.lifecycle.lifecycleScope
import androidx.lifecycle.repeatOnLifecycle
import com.google.mlkit.vision.barcode.BarcodeScanning
import com.google.mlkit.vision.common.InputImage
import com.google.zxing.BarcodeFormat
import com.google.zxing.qrcode.QRCodeWriter
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import org.json.JSONArray
import org.json.JSONObject
import java.io.BufferedReader
import java.io.BufferedWriter
import java.io.File
import java.io.InputStreamReader
import java.io.OutputStreamWriter
import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.ServerSocket
import java.net.Socket
import java.security.KeyFactory
import java.security.KeyPairGenerator
import java.security.PrivateKey
import java.security.PublicKey
import java.security.SecureRandom
import java.security.spec.PKCS8EncodedKeySpec
import java.security.spec.X509EncodedKeySpec
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicBoolean
import javax.crypto.Cipher
import javax.crypto.KeyAgreement
import javax.crypto.SecretKey
import javax.crypto.spec.GCMParameterSpec
import javax.crypto.spec.SecretKeySpec

val MESH_SERVICE_UUID: UUID = UUID.fromString("8a4d5b20-6c91-4e7a-9b34-12f5e6d7c8b9")
const val MESH_SERVICE_NAME = "StoreCarryMeshRFCOMM"
const val MESH_WIFI_PORT = 48889
const val MESH_NSD_SERVICE_TYPE = "_storecarrymesh._tcp."

data class MeshPacket(
    val id: String = UUID.randomUUID().toString(),
    val senderId: String,
    val recipientId: String = "ALL",
    val body: String,
    val hopCount: Int = 0,
    val ttl: Int = 6,
    val createdAtMs: Long = System.currentTimeMillis()
) {
    fun isBroadcast(): Boolean = recipientId.equals("ALL", ignoreCase = true)

    fun isForMe(currentNodeId: String): Boolean =
        isBroadcast() || recipientId == currentNodeId || senderId == currentNodeId

    fun forwardedCopy(): MeshPacket = copy(
        hopCount = hopCount + 1,
        ttl = (ttl - 1).coerceAtLeast(0)
    )

    fun toJson(): String = JSONObject().apply {
        put("id", id)
        put("senderId", senderId)
        put("recipientId", recipientId)
        put("body", body)
        put("hopCount", hopCount)
        put("ttl", ttl)
        put("createdAtMs", createdAtMs)
    }.toString()

    companion object {
        fun fromJson(raw: String): MeshPacket? = try {
            val obj = JSONObject(raw)
            MeshPacket(
                id = obj.getString("id"),
                senderId = obj.getString("senderId"),
                recipientId = obj.optString("recipientId", "ALL"),
                body = obj.getString("body"),
                hopCount = obj.optInt("hopCount", 0),
                ttl = obj.optInt("ttl", 6),
                createdAtMs = obj.optLong("createdAtMs", System.currentTimeMillis())
            )
        } catch (_: Exception) {
            null
        }
    }
}

class MeshDiskStore(context: Context) {
    private val dir = File(context.filesDir, "mesh_store").apply { mkdirs() }
    private val outboxFile = File(dir, "outbox.json")
    private val seenFile = File(dir, "seen_ids.json")

    fun loadOutbox(): MutableList<MeshPacket> {
        if (!outboxFile.exists()) return mutableListOf()
        return try {
            val arr = JSONArray(outboxFile.readText())
            val list = mutableListOf<MeshPacket>()
            for (i in 0 until arr.length()) {
                MeshPacket.fromJson(arr.getString(i))?.let { list.add(it) }
            }
            list
        } catch (_: Exception) {
            mutableListOf()
        }
    }

    fun saveOutbox(packets: List<MeshPacket>) {
        val arr = JSONArray()
        packets.forEach { arr.put(it.toJson()) }
        outboxFile.writeText(arr.toString())
    }

    fun loadSeenIds(): MutableSet<String> {
        if (!seenFile.exists()) return mutableSetOf()
        return try {
            val arr = JSONArray(seenFile.readText())
            val set = mutableSetOf<String>()
            for (i in 0 until arr.length()) {
                set.add(arr.getString(i))
            }
            set
        } catch (_: Exception) {
            mutableSetOf()
        }
    }

    fun saveSeenIds(ids: Set<String>) {
        val arr = JSONArray()
        ids.toList().takeLast(2000).forEach { id -> arr.put(id) }
        seenFile.writeText(arr.toString())
    }
}

class StoreCarryForwardEngine(
    private val store: MeshDiskStore,
    private val nodeId: String
) {
    private val lock = Any()
    private val seenIds: MutableSet<String> = store.loadSeenIds()
    private val outbox: MutableList<MeshPacket> = store.loadOutbox()

    private val _messages = MutableStateFlow<List<MeshPacket>>(outbox.toList())
    val messages: StateFlow<List<MeshPacket>> = _messages.asStateFlow()

    fun createLocalMessage(body: String, recipientId: String = "ALL"): MeshPacket {
        val packet = MeshPacket(
            senderId = nodeId,
            recipientId = recipientId.trim().ifEmpty { "ALL" },
            body = body.trim()
        )
        synchronized(lock) {
            seenIds.add(packet.id)
            outbox.add(packet)
            store.saveSeenIds(seenIds)
            store.saveOutbox(outbox)
            _messages.value = outbox.toList()
        }
        return packet
    }

    fun ingestRemotePacket(incoming: MeshPacket): Boolean {
        synchronized(lock) {
            if (!seenIds.add(incoming.id)) return false
            store.saveSeenIds(seenIds)
            if (incoming.ttl > 0) {
                outbox.add(incoming.forwardedCopy())
                store.saveOutbox(outbox)
                _messages.value = outbox.toList()
            }
            return true
        }
    }

    fun pendingOutboxSnapshot(): List<MeshPacket> = synchronized(lock) { outbox.toList() }
}

data class PeerContact(
    val nodeId: String,
    val publicKeyBase64: String
)

object QrGenerator {
    fun generateQrBitmap(content: String, sizePx: Int = 640): Bitmap {
        val matrix = QRCodeWriter().encode(content, BarcodeFormat.QR_CODE, sizePx, sizePx)
        val bmp = Bitmap.createBitmap(sizePx, sizePx, Bitmap.Config.ARGB_8888)
        for (x in 0 until sizePx) {
            for (y in 0 until sizePx) {
                bmp.setPixel(x, y, if (matrix[x, y]) Color.BLACK else Color.WHITE)
            }
        }
        return bmp
    }
}

class MeshCrypto(context: Context) {
    private val prefs = context.getSharedPreferences("mesh_crypto_prefs", Context.MODE_PRIVATE)
    private val contactsPrefs = context.getSharedPreferences("mesh_contacts_prefs", Context.MODE_PRIVATE)

    init {
        ensureKeyPair()
    }

    private fun ensureKeyPair() {
        if (prefs.contains("priv_x25519") && prefs.contains("pub_x25519")) return
        val kpg = KeyPairGenerator.getInstance("X25519")
        val kp = kpg.generateKeyPair()
        prefs.edit()
            .putString("priv_x25519", Base64.encodeToString(kp.private.encoded, Base64.NO_WRAP))
            .putString("pub_x25519", Base64.encodeToString(kp.public.encoded, Base64.NO_WRAP))
            .apply()
    }

    fun getPublicKeyBase64(): String = prefs.getString("pub_x25519", "")!!

    private fun getPrivateKey(): PrivateKey {
        val raw = Base64.decode(prefs.getString("priv_x25519", "")!!, Base64.NO_WRAP)
        return KeyFactory.getInstance("X25519").generatePrivate(PKCS8EncodedKeySpec(raw))
    }

    private fun decodePublicKey(base64: String): PublicKey {
        val raw = Base64.decode(base64, Base64.NO_WRAP)
        return KeyFactory.getInstance("X25519").generatePublic(X509EncodedKeySpec(raw))
    }

    fun buildQrPayload(nodeId: String): String = JSONObject().apply {
        put("app", "storecarrymesh")
        put("v", 1)
        put("nodeId", nodeId)
        put("pubKey", getPublicKeyBase64())
    }.toString()

    fun saveContactFromQr(rawQr: String): PeerContact? = try {
        val obj = JSONObject(rawQr)
        val peerId = obj.getString("nodeId").trim()
        val pubKey = obj.getString("pubKey").trim()
        if (peerId.isEmpty() || pubKey.isEmpty()) null
        else {
            contactsPrefs.edit().putString(peerId, pubKey).apply()
            PeerContact(peerId, pubKey)
        }
    } catch (_: Exception) {
        null
    }

    fun getContactPublicKey(peerNodeId: String): String? = contactsPrefs.getString(peerNodeId, null)

    fun allContacts(): List<PeerContact> = contactsPrefs.all.mapNotNull { (k, v) ->
        val pub = v as? String
        if (!pub.isNullOrBlank()) PeerContact(k, pub) else null
    }

    private fun deriveAesKey(peerPublicKeyBase64: String): SecretKey {
        val agreement = KeyAgreement.getInstance("X25519")
        agreement.init(getPrivateKey())
        agreement.doPhase(decodePublicKey(peerPublicKeyBase64), true)
        return SecretKeySpec(agreement.generateSecret().copyOf(32), "AES")
    }

    fun encryptForPeer(plainText: String, peerNodeId: String): String? {
        val peerPub = getContactPublicKey(peerNodeId) ?: return null
        val aesKey = deriveAesKey(peerPub)
        val iv = ByteArray(12).also { SecureRandom().nextBytes(it) }
        val cipher = Cipher.getInstance("AES/GCM/NoPadding")
        cipher.init(Cipher.ENCRYPT_MODE, aesKey, GCMParameterSpec(128, iv))
        val cipherBytes = cipher.doFinal(plainText.toByteArray(Charsets.UTF_8))
        return "E2EE:${Base64.encodeToString(iv, Base64.NO_WRAP)}:${Base64.encodeToString(cipherBytes, Base64.NO_WRAP)}"
    }

    fun decryptFromPeer(payload: String, senderNodeId: String): String? {
        if (!payload.startsWith("E2EE:")) return payload
        val parts = payload.removePrefix("E2EE:").split(":")
        if (parts.size != 2) return null
        val peerPub = getContactPublicKey(senderNodeId) ?: return null
        return try {
            val aesKey = deriveAesKey(peerPub)
            val iv = Base64.decode(parts[0], Base64.NO_WRAP)
            val cipherBytes = Base64.decode(parts[1], Base64.NO_WRAP)
            val cipher = Cipher.getInstance("AES/GCM/NoPadding")
            cipher.init(Cipher.DECRYPT_MODE, aesKey, GCMParameterSpec(128, iv))
            String(cipher.doFinal(cipherBytes), Charsets.UTF_8)
        } catch (_: Exception) {
            null
        }
    }
}

class BluetoothMeshTransport(
    private val bluetoothAdapter: BluetoothAdapter?,
    private val engine: StoreCarryForwardEngine,
    private val scope: CoroutineScope,
    private val onStatus: (String) -> Unit
) {
    private var serverJob: Job? = null
    private var syncJob: Job? = null
    private var serverSocket: BluetoothServerSocket? = null

    @SuppressLint("MissingPermission")
    fun start() {
        stop()
        val adapter = bluetoothAdapter ?: run {
            onStatus("BT недоступний")
            return
        }
        if (!adapter.isEnabled) {
            onStatus("Увімкніть Bluetooth")
            return
        }

        serverJob = scope.launch(Dispatchers.IO) {
            onStatus("BT RFCOMM очікує з'єднань...")
            while (isActive) {
                try {
                    val srv = adapter.listenUsingRfcommWithServiceRecord(MESH_SERVICE_NAME, MESH_SERVICE_UUID)
                    serverSocket = srv
                    val socket = srv.accept()
                    srv.close()
                    handleSocket(socket)
                } catch (_: Exception) {
                    try { serverSocket?.close() } catch (_: Exception) {}
                    delay(1500)
                }
            }
        }

        syncJob = scope.launch(Dispatchers.IO) {
            while (isActive) {
                val bonded: Set<BluetoothDevice> = try {
                    adapter.bondedDevices ?: emptySet()
                } catch (_: SecurityException) {
                    onStatus("Немає дозволу BLUETOOTH_CONNECT")
                    emptySet()
                }
                val packets = engine.pendingOutboxSnapshot()
                if (packets.isNotEmpty()) {
                    for (device in bonded) {
                        if (!isActive) break
                        trySyncWithDevice(device, packets)
                    }
                }
                delay(8000)
            }
        }
    }

    @SuppressLint("MissingPermission")
    private fun trySyncWithDevice(device: BluetoothDevice, packets: List<MeshPacket>) {
        var socket: BluetoothSocket? = null
        try {
            socket = device.createRfcommSocketToServiceRecord(MESH_SERVICE_UUID)
            socket.connect()
            val writer = BufferedWriter(OutputStreamWriter(socket.outputStream, Charsets.UTF_8))
            for (pkt in packets) {
                writer.write(pkt.toJson())
                writer.newLine()
            }
            writer.flush()
            onStatus("BT: передано ${packets.size} пакетів -> ${device.name ?: device.address}")
        } catch (_: Exception) {
        } finally {
            try { socket?.close() } catch (_: Exception) {}
        }
    }

    private fun handleSocket(socket: BluetoothSocket) {
        try {
            socket.use { sock ->
                val reader = BufferedReader(InputStreamReader(sock.inputStream, Charsets.UTF_8))
                var added = 0
                while (true) {
                    val line = reader.readLine() ?: break
                    val pkt = MeshPacket.fromJson(line) ?: continue
                    if (engine.ingestRemotePacket(pkt)) added++
                }
                if (added > 0) onStatus("BT: прийнято/ретрансльовано пакетів: $added")
            }
        } catch (_: Exception) {
        }
    }

    fun stop() {
        serverJob?.cancel()
        syncJob?.cancel()
        try { serverSocket?.close() } catch (_: Exception) {}
    }
}

class WifiMeshTransport(
    private val context: Context,
    private val nodeId: String,
    private val engine: StoreCarryForwardEngine,
    private val scope: CoroutineScope,
    private val onStatus: (String) -> Unit
) {
    private val nsdManager = context.getSystemService(Context.NSD_SERVICE) as NsdManager
    private val discoveredIps = ConcurrentHashMap<String, Long>()

    private var serverJob: Job? = null
    private var syncJob: Job? = null
    private var serverSocket: ServerSocket? = null
    private var registrationListener: NsdManager.RegistrationListener? = null
    private var discoveryListener: NsdManager.DiscoveryListener? = null

    fun start() {
        stop()
        registerAndDiscoverNsd()

        serverJob = scope.launch(Dispatchers.IO) {
            try {
                val srv = ServerSocket(MESH_WIFI_PORT)
                serverSocket = srv
                onStatus("Wi-Fi ретранслятор активний (порт $MESH_WIFI_PORT)")
                while (isActive) {
                    val client = srv.accept()
                    launch(Dispatchers.IO) { handleClient(client) }
                }
            } catch (_: Exception) {
                delay(2000)
            }
        }

        syncJob = scope.launch(Dispatchers.IO) {
            while (isActive) {
                val now = System.currentTimeMillis()
                discoveredIps.entries.removeIf { now - it.value > 60_000L }
                val packets = engine.pendingOutboxSnapshot()
                if (packets.isNotEmpty()) {
                    for (ip in discoveredIps.keys.toList()) {
                        if (!isActive) break
                        tryPushToIp(ip, packets)
                    }
                }
                delay(6000)
            }
        }
    }

    private fun registerAndDiscoverNsd() {
        val serviceInfo = NsdServiceInfo().apply {
            serviceName = "MeshNode-${nodeId.take(10)}-${(1000..9999).random()}"
            serviceType = MESH_NSD_SERVICE_TYPE
            port = MESH_WIFI_PORT
        }

        val regListener = object : NsdManager.RegistrationListener {
            override fun onServiceRegistered(info: NsdServiceInfo) {}
            override fun onRegistrationFailed(info: NsdServiceInfo, errorCode: Int) {}
            override fun onServiceUnregistered(info: NsdServiceInfo) {}
            override fun onUnregistrationFailed(info: NsdServiceInfo, errorCode: Int) {}
        }
        registrationListener = regListener
        try { nsdManager.registerService(serviceInfo, NsdManager.PROTOCOL_DNS_SD, regListener) } catch (_: Exception) {}

        val discListener = object : NsdManager.DiscoveryListener {
            override fun onDiscoveryStarted(regType: String) {}
            override fun onServiceFound(service: NsdServiceInfo) {
                if (service.serviceType != MESH_NSD_SERVICE_TYPE) return
                if (service.serviceName.startsWith("MeshNode-${nodeId.take(10)}-")) return
                nsdManager.resolveService(service, object : NsdManager.ResolveListener {
                    override fun onResolveFailed(svc: NsdServiceInfo, errorCode: Int) {}
                    override fun onServiceResolved(svc: NsdServiceInfo) {
                        val host: InetAddress? = svc.host
                        val ip = host?.hostAddress
                        if (!ip.isNullOrBlank() && !ip.contains(":")) {
                            discoveredIps[ip] = System.currentTimeMillis()
                            onStatus("Wi-Fi: знайдено вузол $ip")
                        }
                    }
                })
            }
            override fun onServiceLost(service: NsdServiceInfo) {}
            override fun onDiscoveryStopped(serviceType: String) {}
            override fun onStartDiscoveryFailed(serviceType: String, errorCode: Int) {}
            override fun onStopDiscoveryFailed(serviceType: String, errorCode: Int) {}
        }
        discoveryListener = discListener
        try { nsdManager.discoverServices(MESH_NSD_SERVICE_TYPE, NsdManager.PROTOCOL_DNS_SD, discListener) } catch (_: Exception) {}
    }

    private fun tryPushToIp(ip: String, packets: List<MeshPacket>) {
        var socket: Socket? = null
        try {
            socket = Socket()
            socket.connect(InetSocketAddress(ip, MESH_WIFI_PORT), 2500)
            socket.soTimeout = 3000
            val writer = BufferedWriter(OutputStreamWriter(socket.getOutputStream(), Charsets.UTF_8))
            for (pkt in packets) {
                writer.write(pkt.toJson())
                writer.newLine()
            }
            writer.flush()
            onStatus("Wi-Fi: передано ${packets.size} пакетів -> $ip")
        } catch (_: Exception) {
        } finally {
            try { socket?.close() } catch (_: Exception) {}
        }
    }

    private fun handleClient(client: Socket) {
        try {
            client.use { sock ->
                sock.soTimeout = 4000
                val reader = BufferedReader(InputStreamReader(sock.getInputStream(), Charsets.UTF_8))
                var added = 0
                while (true) {
                    val line = reader.readLine() ?: break
                    val pkt = MeshPacket.fromJson(line) ?: continue
                    if (engine.ingestRemotePacket(pkt)) added++
                }
                if (added > 0) onStatus("Wi-Fi: прийнято/ретрансльовано пакетів: $added")
            }
        } catch (_: Exception) {
        }
    }

    fun stop() {
        serverJob?.cancel()
        syncJob?.cancel()
        try { serverSocket?.close() } catch (_: Exception) {}
        registrationListener?.let { try { nsdManager.unregisterService(it) } catch (_: Exception) {} }
        discoveryListener?.let { try { nsdManager.stopServiceDiscovery(it) } catch (_: Exception) {} }
        registrationListener = null
        discoveryListener = null
    }
}

class QrScannerDialog(
    context: Context,
    private val lifecycleOwner: LifecycleOwner,
    private val onQrScanned: (String) -> Unit
) : Dialog(context) {

    private var cameraExecutor: ExecutorService? = null
    private val handled = AtomicBoolean(false)

    @OptIn(ExperimentalGetImage::class)
    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)

        val container = LinearLayout(context).apply {
            orientation = LinearLayout.VERTICAL
            setPadding(24, 24, 24, 24)
        }
        val previewView = PreviewView(context).apply {
            layoutParams = LinearLayout.LayoutParams(LinearLayout.LayoutParams.MATCH_PARENT, 900)
        }
        container.addView(previewView)
        container.addView(Button(context).apply {
            text = "Закрити сканер"
            setOnClickListener { dismiss() }
        })
        setContentView(container)

        val executor = Executors.newSingleThreadExecutor()
        cameraExecutor = executor
        val scanner = BarcodeScanning.getClient()
        val providerFuture = ProcessCameraProvider.getInstance(context)

        providerFuture.addListener({
            val cameraProvider = providerFuture.get()
            val preview = Preview.Builder().build().also { it.setSurfaceProvider(previewView.surfaceProvider) }
            val analysis = ImageAnalysis.Builder()
                .setTargetResolution(Size(1280, 720))
                .setBackpressureStrategy(ImageAnalysis.STRATEGY_KEEP_ONLY_LATEST)
                .build()

            analysis.setAnalyzer(executor) { imageProxy: ImageProxy ->
                val mediaImage = imageProxy.image
                if (mediaImage != null && !handled.get()) {
                    val inputImage = InputImage.fromMediaImage(mediaImage, imageProxy.imageInfo.rotationDegrees)
                    scanner.process(inputImage)
                        .addOnSuccessListener { barcodes ->
                            val raw = barcodes.firstOrNull()?.rawValue
                            if (!raw.isNullOrBlank() && handled.compareAndSet(false, true)) {
                                dismiss()
                                onQrScanned(raw)
                            }
                        }
                        .addOnCompleteListener { imageProxy.close() }
                } else {
                    imageProxy.close()
                }
            }

            try {
                cameraProvider.unbindAll()
                cameraProvider.bindToLifecycle(lifecycleOwner, CameraSelector.DEFAULT_BACK_CAMERA, preview, analysis)
            } catch (_: Exception) {
                dismiss()
            }
        }, ContextCompat.getMainExecutor(context))
    }

    override fun dismiss() {
        super.dismiss()
        cameraExecutor?.shutdown()
        cameraExecutor = null
    }
}

class MainActivity : ComponentActivity() {

    private lateinit var nodeId: String
    private lateinit var crypto: MeshCrypto
    private lateinit var store: MeshDiskStore
    private lateinit var engine: StoreCarryForwardEngine
    private var btTransport: BluetoothMeshTransport? = null
    private var wifiTransport: WifiMeshTransport? = null

    private lateinit var statusText: TextView
    private lateinit var contactsText: TextView
    private lateinit var messagesText: TextView
    private lateinit var recipientInput: EditText

    private val permissionsLauncher =
        registerForActivityResult(ActivityResultContracts.RequestMultiplePermissions()) { grantResults ->
            val cameraGranted = grantResults[Manifest.permission.CAMERA] == true ||
                ContextCompat.checkSelfPermission(this, Manifest.permission.CAMERA) == PackageManager.PERMISSION_GRANTED
            if (cameraGranted) openQrScanner()
            startTransports()
        }

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)

        nodeId = getOrCreateNodeId()
        crypto = MeshCrypto(applicationContext)
        store = MeshDiskStore(applicationContext)
        engine = StoreCarryForwardEngine(store = store, nodeId = nodeId)

        val root = LinearLayout(this).apply {
            orientation = LinearLayout.VERTICAL
            setPadding(32, 32, 32, 32)
        }

        root.addView(TextView(this).apply {
            text = "Вузол: $nodeId (BT + Wi-Fi ретранслятор)"
            textSize = 15f
        })

        statusText = TextView(this).apply {
            text = "Статус: запуск..."
            textSize = 13f
            setPadding(0, 8, 0, 12)
        }
        root.addView(statusText)

        val qrRow = LinearLayout(this).apply { orientation = LinearLayout.HORIZONTAL }
        qrRow.addView(Button(this).apply {
            text = "Мій QR-код"
            setOnClickListener { showMyQrDialog() }
        })
        qrRow.addView(Button(this).apply {
            text = "Сканувати QR друга"
            setOnClickListener { requestCameraAndScan() }
        })
        root.addView(qrRow)

        contactsText = TextView(this).apply {
            textSize = 12f
            setPadding(0, 8, 0, 8)
        }
        refreshContactsLabel()
        root.addView(contactsText)

        recipientInput = EditText(this).apply {
            hint = "nodeId друга (після QR) або ALL для всіх"
            inputType = InputType.TYPE_CLASS_TEXT
            singleLine = true
        }
        root.addView(recipientInput)

        val messageInput = EditText(this).apply {
            hint = "Повідомлення..."
            inputType = InputType.TYPE_CLASS_TEXT or InputType.TYPE_TEXT_FLAG_CAP_SENTENCES
            minLines = 2
        }
        root.addView(messageInput)

        root.addView(Button(this).apply {
            text = "Надіслати у mesh"
            setOnClickListener {
                val plain = messageInput.text.toString().trim()
                if (plain.isEmpty()) {
                    statusText.text = "Статус: введіть текст"
                    return@setOnClickListener
                }
                val target = recipientInput.text.toString().trim().ifEmpty { "ALL" }
                val bodyToStore = if (target.equals("ALL", ignoreCase = true)) {
                    plain
                } else {
                    crypto.encryptForPeer(plain, target) ?: run {
                        statusText.text = "Статус: спочатку відскануйте QR вузла $target"
                        return@setOnClickListener
                    }
                }
                val pkt = engine.createLocalMessage(body = bodyToStore, recipientId = target)
                messageInput.text.clear()
                statusText.text = "Статус: додано у буфер -> $target (ttl=${pkt.ttl})"
            }
        })

        val scroll = ScrollView(this).apply { setPadding(0, 16, 0, 0) }
        messagesText = TextView(this).apply {
            text = "Повідомлень поки немає"
            textSize = 14f
        }
        scroll.addView(messagesText)
        root.addView(scroll)
        setContentView(root)

        lifecycleScope.launch {
            repeatOnLifecycle(Lifecycle.State.STARTED) {
                engine.messages.collect { packets ->
                    val mineOrBroadcast = packets.filter { it.isForMe(nodeId) }
                    messagesText.text = if (mineOrBroadcast.isEmpty()) {
                        "Немає повідомлень для цього вузла (чужі пакети ретранслюються)"
                    } else {
                        mineOrBroadcast.reversed().joinToString("\n\n") { pkt ->
                            val shown = if (pkt.body.startsWith("E2EE:")) {
                                val otherId = if (pkt.senderId == nodeId) pkt.recipientId else pkt.senderId
                                crypto.decryptFromPeer(pkt.body, otherId) ?: "[Зашифровано]"
                            } else {
                                pkt.body
                            }
                            "[${pkt.senderId} -> ${pkt.recipientId} | hop=${pkt.hopCount}, ttl=${pkt.ttl}]\n$shown"
                        }
                    }
                }
            }
        }

        ensurePermissionsAndStart()
    }

    override fun onDestroy() {
        btTransport?.stop()
        wifiTransport?.stop()
        super.onDestroy()
    }

    private fun showMyQrDialog() {
        val bitmap = QrGenerator.generateQrBitmap(crypto.buildQrPayload(nodeId))
        val layout = LinearLayout(this).apply {
            orientation = LinearLayout.VERTICAL
            setPadding(36, 36, 36, 36)
        }
        layout.addView(ImageView(this).apply {
            setImageBitmap(bitmap)
            adjustViewBounds = true
        })
        layout.addView(TextView(this).apply {
            text = "nodeId: $nodeId"
            setPadding(0, 16, 0, 0)
        })
        AlertDialog.Builder(this)
            .setView(layout)
            .setPositiveButton("Закрити", null)
            .show()
    }

    private fun requestCameraAndScan() {
        if (ContextCompat.checkSelfPermission(this, Manifest.permission.CAMERA) == PackageManager.PERMISSION_GRANTED) {
            openQrScanner()
        } else {
            permissionsLauncher.launch(arrayOf(Manifest.permission.CAMERA))
        }
    }

    private fun openQrScanner() {
        QrScannerDialog(this, this) { raw ->
            val contact = crypto.saveContactFromQr(raw)
            if (contact != null) {
                recipientInput.setText(contact.nodeId)
                refreshContactsLabel()
                statusText.text = "Статус: додано контакт ${contact.nodeId}"
            } else {
                statusText.text = "Статус: невідомий QR-код"
            }
        }.show()
    }

    private fun refreshContactsLabel() {
        val list = crypto.allContacts()
        contactsText.text = if (list.isEmpty()) "Контакти: немає" else "Контакти: ${list.joinToString { it.nodeId }}"
    }

    private fun getOrCreateNodeId(): String {
        val prefs = getSharedPreferences("mesh_prefs", Context.MODE_PRIVATE)
        val existing = prefs.getString("node_id", null)
        if (!existing.isNullOrBlank()) return existing
        val generated = "node-${UUID.randomUUID().toString().take(6)}"
        prefs.edit().putString("node_id", generated).apply()
        return generated
    }

    private fun ensurePermissionsAndStart() {
        val needed = mutableListOf<String>()
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.S) {
            needed += Manifest.permission.BLUETOOTH_CONNECT
            needed += Manifest.permission.BLUETOOTH_SCAN
        }
        val missing = needed.filter { ContextCompat.checkSelfPermission(this, it) != PackageManager.PERMISSION_GRANTED }
        if (missing.isNotEmpty()) {
            permissionsLauncher.launch(missing.toTypedArray())
            return
        }
        startTransports()
    }

    private fun startTransports() {
        val adapter = (getSystemService(Context.BLUETOOTH_SERVICE) as? BluetoothManager)?.adapter
        btTransport?.stop()
        btTransport = BluetoothMeshTransport(
            bluetoothAdapter = adapter,
            engine = engine,
            scope = lifecycleScope,
            onStatus = { msg -> runOnUiThread { statusText.text = "Статус: $msg" } }
        ).also { it.start() }

        wifiTransport?.stop()
        wifiTransport = WifiMeshTransport(
            context = applicationContext,
            nodeId = nodeId,
            engine = engine,
            scope = lifecycleScope,
            onStatus = { msg -> runOnUiThread { statusText.text = "Статус: $msg" } }
        ).also { it.start() }
    }
}
EOF

git add -A && git commit -m "Consolidate mesh, E2EE, QR, BT RFCOMM and Wi-Fi NSD into AllInOneMesh.kt" && git push -u origin main
 Red
Связи без інтернету 
