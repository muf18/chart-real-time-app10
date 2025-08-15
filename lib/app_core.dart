// file_path: lib/app_core.dart
// ignore_for_file: avoid_print, unnecessary_brace_in_string_interps

import 'dart:async';
import 'dart:collection';
import 'dart:convert';
import 'dart:io';
import 'dart:isolate';
import 'dart:math';
import 'dart:typed_data';

import 'package:http/http.dart' as http;
import 'package:logging/logging.dart';
import 'package:rate_limiter/rate_limiter.dart';
import 'package:web_socket_channel/io.dart';
import 'package:decimal/decimal.dart';

// Entry point for the core isolate
void coreIsolateMain(SendPort sendPort) {
  final controller = CoreController(sendPort);
  final receivePort = ReceivePort();
  sendPort.send(receivePort.sendPort);

  receivePort.listen((message) {
    controller.handleCommand(message);
  });
}

// --- CONSTANTS ---
const _scaleFactor = 100000000;
const _scaleFactorDecimal = 100000000.0;

const Map<String, int> _timeframeSeconds = {
  "1m": 60,
  "5m": 300,
  "15m": 900,
  "30m": 1800,
  "1h": 3600,
  "4h": 14400,
  "1d": 86400,
  "1w": 604800,
};

const List<String> _supportedTimeframes = [
  "1m",
  "5m",
  "15m",
  "30m",
  "1h",
  "4h",
  "1d",
  "1w"
];

const Map<String, Map<String, String>> _symbolNormalizationMap = {
  "Binance": {"BTC/USDT": "BTCUSDT"},
  "OKX": {"BTC/USDT": "BTC-USDT"},
  "Bitget": {"BTC/USDT": "BTCUSDT"},
  "Coinbase Exchange": {"BTC/USD": "BTC-USD"},
  "Bitstamp": {"BTC/USD": "btcusd"},
  "Kraken": {"BTC/USD": "XBT/USD", "BTC/EUR": "XBT/EUR"},
  "Bitvavo": {"BTC/EUR": "BTC-EUR"},
};

// --- UTILITY FUNCTIONS ---
int _doubleToInt(double val) => (val * _scaleFactorDecimal).round();
int _stringToInt(String val) => _doubleToInt(double.parse(val));
int _decimalToInt(Decimal val) =>
    (val * Decimal.fromInt(_scaleFactor)).toBigInt().toInt();
Decimal _intToDecimal(int val) =>
    Decimal.fromInt(val) / Decimal.fromInt(_scaleFactor);

// --- DATA MODELS ---
class NormalizedTrade {
  final String symbol;
  final String venue;
  final int priceInt;
  final int sizeInt;
  final int timestampUtcNs;

  NormalizedTrade({
    required this.symbol,
    required this.venue,
    required this.priceInt,
    required this.sizeInt,
    required this.timestampUtcNs,
  });

  @override
  String toString() =>
      'NormalizedTrade(symbol: $symbol, venue: $venue, price: ${_intToDecimal(priceInt)}, size: ${_intToDecimal(sizeInt)}, ts: $timestampUtcNs)';
  @override
  bool operator ==(Object other) =>
      identical(this, other) ||
      other is NormalizedTrade &&
          runtimeType == other.runtimeType &&
          timestampUtcNs == other.timestampUtcNs &&
          venue == other.venue;
  @override
  int get hashCode => timestampUtcNs.hashCode ^ venue.hashCode;
}

class AggregatedDataPoint {
  final String symbol;
  final String timeframe;
  final int timestampUtcS;
  final int vwapInt;
  final int volumeInt;
  final int lastPriceInt;
  final bool amend;

  AggregatedDataPoint({
    required this.symbol,
    required this.timeframe,
    required this.timestampUtcS,
    required this.vwapInt,
    required this.volumeInt,
    required this.lastPriceInt,
    required this.amend,
  });

  Map<String, dynamic> toJson() => {
        'symbol': symbol,
        'timeframe': timeframe,
        'timestampUtcS': timestampUtcS,
        'vwapInt': vwapInt,
        'volumeInt': volumeInt,
        'lastPriceInt': lastPriceInt,
        'amend': amend,
      };
  @override
  String toString() =>
      'AggregatedDataPoint(symbol: $symbol, timeframe: $timeframe, ts: $timestampUtcS, vwap: ${_intToDecimal(vwapInt)}, vol: ${_intToDecimal(volumeInt)}, lastPx: ${_intToDecimal(lastPriceInt)}, amend: $amend)';
}

class Candle {
  final String symbol;
  final String timeframe;
  final int openTimeUtcS;
  final int openInt;
  final int highInt;
  final int lowInt;
  final int closeInt;
  final int volumeInt;

  Candle({
    required this.symbol,
    required this.timeframe,
    required this.openTimeUtcS,
    required this.openInt,
    required this.highInt,
    required this.lowInt,
    required this.closeInt,
    required this.volumeInt,
  });

  Map<String, dynamic> toJson() => {
        'symbol': symbol,
        'timeframe': timeframe,
        'openTimeUtcS': openTimeUtcS,
        'openInt': openInt,
        'highInt': highInt,
        'lowInt': lowInt,
        'closeInt': closeInt,
        'volumeInt': volumeInt,
      };
  @override
  String toString() =>
      'Candle(symbol: $symbol, timeframe: $timeframe, time: $openTimeUtcS, o: ${_intToDecimal(openInt)}, h: ${_intToDecimal(highInt)}, l: ${_intToDecimal(lowInt)}, c: ${_intToDecimal(closeInt)}, v: ${_intToDecimal(volumeInt)})';
  @override
  bool operator ==(Object other) =>
      identical(this, other) ||
      other is Candle &&
          runtimeType == other.runtimeType &&
          openTimeUtcS == other.openTimeUtcS &&
          symbol == other.symbol &&
          timeframe == other.timeframe;
  @override
  int get hashCode =>
      openTimeUtcS.hashCode ^ symbol.hashCode ^ timeframe.hashCode;
}

// --- CORE CONTROLLER ---
class CoreController {
  final SendPort _uiSendPort;
  final Logger _log = Logger('CoreController');
  bool _debug = false;

  final Map<String, ExchangeAdapter> _adapters = {};
  final http.Client _httpClient = http.Client();
  late final Aggregator _aggregator;
  late final StateManager _stateManager;

  String _currentSymbol = 'BTC/USDT';
  String _currentTimeframe = '1m';

  CoreController(this._uiSendPort) {
    _aggregator = Aggregator(this._sendMessage);
    _setupLogging();
  }

  void _setupLogging() {
    Logger.root.level = Level.INFO;
    Logger.root.onRecord.listen((record) {
      if (_debug) {
        print(
            '${record.level.name}: ${record.time}: ${record.loggerName}: ${record.message}');
      }
    });
  }

  void handleCommand(dynamic message) {
    try {
      final Map<String, dynamic> command = jsonDecode(message);
      final type = command['type'] as String;
      final reqId = command['req_id'] as String?;

      _log.info("Received command: $type");

      switch (type) {
        case 'init':
          _handleInit(command);
          break;
        case 'setSymbol':
          _handleSetSymbol(command['symbol'] as String, reqId);
          break;
        case 'setTimeframe':
          _handleSetTimeframe(command['timeframe'] as String, reqId);
          break;
        case 'backfill':
          _handleBackfill(command, reqId);
          break;
        case 'shutdown':
          _handleShutdown();
          break;
        default:
          _sendError('UNKNOWN_CMD', 'Unknown command type: $type',
              reqId: reqId);
      }
    } catch (e, s) {
      _sendError('COMMAND_ERROR', 'Failed to handle command: $e',
          details: s.toString());
    }
  }

  Future<void> _handleInit(Map<String, dynamic> command) async {
    _debug = command['debug'] as bool? ?? false;
    final stateDirPath = command['stateDirPath'] as String;
    _stateManager = StateManager(stateDirPath);

    final initialState = await _stateManager.loadState();
    if (initialState != null) {
      _currentSymbol = initialState['lastSymbol'] ?? _currentSymbol;
      _currentTimeframe = initialState['lastTimeframe'] ?? _currentTimeframe;
    }

    _initializeAdapters();
    await _subscribeToCurrentSymbol();
    _aggregator.start(_currentSymbol);
    _sendAck('init', true, reqId: command['req_id']);
  }

  void _initializeAdapters() {
    final tradeStreamController = StreamController<NormalizedTrade>.broadcast();
    final tradeStream = tradeStreamController.stream;

    _adapters['Binance'] = BinanceAdapter(
        tradeStreamController, _httpClient, _log, this._sendMessage);
    _adapters['OKX'] =
        OKXAdapter(tradeStreamController, _httpClient, _log, this._sendMessage);
    _adapters['Bitget'] = BitgetAdapter(
        tradeStreamController, _httpClient, _log, this._sendMessage);
    _adapters['Coinbase Exchange'] = CoinbaseAdapter(
        tradeStreamController, _httpClient, _log, this._sendMessage);
    _adapters['Bitstamp'] = BitstampAdapter(
        tradeStreamController, _httpClient, _log, this._sendMessage);
    _adapters['Kraken'] = KrakenAdapter(
        tradeStreamController, _httpClient, _log, this._sendMessage);
    _adapters['Bitvavo'] = BitvavoAdapter(
        tradeStreamController, _httpClient, _log, this._sendMessage);

    tradeStream.listen(_aggregator.addTrade);
  }

  Future<void> _handleSetSymbol(String symbol, String? reqId) async {
    if (_currentSymbol == symbol) {
      _sendAck('setSymbol', true, reqId: reqId);
      return;
    }
    _currentSymbol = symbol;
    await _stateManager.saveState(
        {'lastSymbol': _currentSymbol, 'lastTimeframe': _currentTimeframe});
    _aggregator.switchSymbol(symbol);
    await _subscribeToCurrentSymbol();
    _sendAck('setSymbol', true, reqId: reqId);
  }

  Future<void> _handleSetTimeframe(String timeframe, String? reqId) async {
    if (!_supportedTimeframes.contains(timeframe)) {
      _sendError('INVALID_ARG', 'Unsupported timeframe: $timeframe',
          reqId: reqId);
      return;
    }
    if (_currentTimeframe == timeframe) {
      _sendAck('setTimeframe', true, reqId: reqId);
      return;
    }
    _currentTimeframe = timeframe;
    await _stateManager.saveState(
        {'lastSymbol': _currentSymbol, 'lastTimeframe': _currentTimeframe});
    _sendAck('setTimeframe', true, reqId: reqId);
  }

  Future<void> _handleBackfill(
      Map<String, dynamic> command, String? reqId) async {
    try {
      final symbol = command['symbol'] as String;
      final timeframe = command['timeframe'] as String;
      final start = DateTime.parse(command['startIso'] as String);
      final end = DateTime.parse(command['endIso'] as String);

      if (start.isAfter(end)) {
        _sendError('INVALID_ARG', 'startIso must be before endIso',
            reqId: reqId);
        return;
      }

      // Find a suitable adapter for the symbol
      final adapter = _adapters.values.firstWhere(
        (a) => _symbolNormalizationMap[a.name]?.containsKey(symbol) ?? false,
        orElse: () => _adapters.values.first, // Fallback, not ideal
      );

      _log.info(
          "Backfilling $symbol on ${adapter.name} from $start to $end for $timeframe");
      final candles =
          await adapter.fetchHistoricalCandles(symbol, timeframe, start, end);
      for (final candle in candles) {
        _sendMessage('candle', candle.toJson());
      }
      _sendAck('backfill', true, reqId: reqId);
    } catch (e, s) {
      _sendError('BACKFILL_ERROR', 'Failed to backfill: $e',
          details: s.toString(), reqId: reqId);
    }
  }

  Future<void> _subscribeToCurrentSymbol() async {
    await Future.wait(_adapters.values.map((a) => a.disconnect()));
    final relevantAdapters = _adapters.values.where((a) =>
        _symbolNormalizationMap[a.name]?.containsKey(_currentSymbol) ?? false);
    await Future.wait(
        relevantAdapters.map((a) => a.connectAndSubscribe(_currentSymbol)));
  }

  void _handleShutdown() {
    _log.info("Shutdown command received. Closing connections.");
    Future.wait(_adapters.values.map((a) => a.disconnect())).whenComplete(() {
      _httpClient.close();
      _aggregator.stop();
      Isolate.current.kill();
    });
  }

  void _sendMessage(String type, dynamic data, {String? reqId}) {
    final message = {
      'type': type,
      'data': data,
      'ts': DateTime.now().toUtc().toIso8601String(),
      if (reqId != null) 'req_id': reqId,
    };
    _uiSendPort.send(jsonEncode(message));
  }

  void _sendError(String code, String message,
      {dynamic details, String? reqId}) {
    _log.severe("ERROR: $code - $message - $details");
    _sendMessage(
        'error', {'code': code, 'message': message, 'details': details},
        reqId: reqId);
  }

  void _sendAck(String commandType, bool ok, {String? message, String? reqId}) {
    _sendMessage('ack',
        {'for': commandType, 'ok': ok, if (message != null) 'message': message},
        reqId: reqId);
  }
}

// --- STATE MANAGER ---
class StateManager {
  final String _stateDirPath;
  File get _stateFile => File('$_stateDirPath/app_state.json');

  StateManager(this._stateDirPath);

  Future<Map<String, dynamic>?> loadState() async {
    try {
      if (await _stateFile.exists()) {
        final content = await _stateFile.readAsString();
        return jsonDecode(content) as Map<String, dynamic>;
      }
    } catch (e) {
      print("Error loading state: $e");
    }
    return null;
  }

  Future<void> saveState(Map<String, dynamic> state) async {
    try {
      final tempFile = File('${_stateDirPath}/app_state.json.tmp');
      await tempFile.writeAsString(jsonEncode(state));
      await tempFile.rename(_stateFile.path);
    } catch (e) {
      print("Error saving state: $e");
    }
  }
}

// --- AGGREGATOR ---
class Aggregator {
  final void Function(String type, dynamic data) _sendMessage;
  Timer? _timer;
  String _currentSymbol = '';
  final _tradeQueue = Queue<NormalizedTrade>();
  final Map<String, _TimeframeAggregator> _timeframeAggregators = {};

  Aggregator(this._sendMessage);

  void start(String symbol) {
    _currentSymbol = symbol;
    _timeframeAggregators.clear();
    for (var tf in _timeframeSeconds.entries) {
      _timeframeAggregators[tf.key] =
          _TimeframeAggregator(symbol, tf.key, tf.value, _sendMessage);
    }
    _timer?.cancel();
    _timer = Timer.periodic(const Duration(milliseconds: 250), _processQueue);
  }

  void stop() {
    _timer?.cancel();
  }

  void switchSymbol(String newSymbol) {
    _currentSymbol = newSymbol;
    _tradeQueue.clear();
    _timeframeAggregators.clear();
    for (var tf in _timeframeSeconds.entries) {
      _timeframeAggregators[tf.key] =
          _TimeframeAggregator(newSymbol, tf.key, tf.value, _sendMessage);
    }
  }

  void addTrade(NormalizedTrade trade) {
    if (trade.symbol == _currentSymbol) {
      _tradeQueue.add(trade);
    }
  }

  void _processQueue(Timer timer) {
    final now = DateTime.now().toUtc();
    final nowNs = now.microsecondsSinceEpoch * 1000;

    while (_tradeQueue.isNotEmpty) {
      final trade = _tradeQueue.removeFirst();
      for (final aggregator in _timeframeAggregators.values) {
        aggregator.processTrade(trade);
      }
    }

    for (final aggregator in _timeframeAggregators.values) {
      aggregator.tick(nowNs);
    }
  }
}

class _TimeframeAggregator {
  final String symbol;
  final String timeframe;
  final int timeframeSec;
  final void Function(String type, dynamic data) _sendMessage;

  int _currentBucketStart = 0;
  BigInt _pvSumInt = BigInt.zero;
  BigInt _vSumInt = BigInt.zero;
  int _lastPriceInt = 0;

  _TimeframeAggregator(
      this.symbol, this.timeframe, this.timeframeSec, this._sendMessage);

  void processTrade(NormalizedTrade trade) {
    final tradeTimeS = trade.timestampUtcNs ~/ 1000000000;
    final bucketStart = (tradeTimeS ~/ timeframeSec) * timeframeSec;

    if (_currentBucketStart == 0) {
      _currentBucketStart = bucketStart;
    }

    if (bucketStart == _currentBucketStart) {
      _pvSumInt += BigInt.from(trade.priceInt) * BigInt.from(trade.sizeInt);
      _vSumInt += BigInt.from(trade.sizeInt);
      _lastPriceInt = trade.priceInt;
    } else if (bucketStart == _currentBucketStart - timeframeSec) {
      // Late trade for previous bucket
      _pvSumInt += BigInt.from(trade.priceInt) * BigInt.from(trade.sizeInt);
      _vSumInt += BigInt.from(trade.sizeInt);
      _lastPriceInt = trade.priceInt;
      _emit(amend: true);
    } else {
      // Trade is too late or for a future bucket, just update last price
      _lastPriceInt = trade.priceInt;
    }
  }

  void tick(int nowNs) {
    final nowS = nowNs ~/ 1000000000;
    if (_currentBucketStart > 0 && nowS >= _currentBucketStart + timeframeSec) {
      _emit(amend: false);
      _resetBucket(nowS);
    }
  }

  void _emit({required bool amend}) {
    if (_vSumInt > BigInt.zero) {
      final vwapInt = (_pvSumInt ~/ _vSumInt).toInt();
      final dataPoint = AggregatedDataPoint(
        symbol: symbol,
        timeframe: timeframe,
        timestampUtcS: _currentBucketStart,
        vwapInt: vwapInt,
        volumeInt: (_vSumInt ~/ BigInt.from(_scaleFactor)).toInt(),
        lastPriceInt: _lastPriceInt,
        amend: amend,
      );
      _sendMessage('aggregated', dataPoint.toJson());
    }
  }

  void _resetBucket(int nowS) {
    _currentBucketStart = (nowS ~/ timeframeSec) * timeframeSec;
    _pvSumInt = BigInt.zero;
    _vSumInt = BigInt.zero;
  }
}

// --- EXCHANGE ADAPTERS ---
abstract class ExchangeAdapter {
  final String name;
  final StreamController<NormalizedTrade> _tradeController;
  final http.Client _httpClient;
  final Logger _log;
  final void Function(String type, dynamic data) _sendMessage;

  IOWebSocketChannel? _ws;
  StreamSubscription? _wsSubscription;
  Timer? _heartbeatTimer;
  Timer? _reconnectTimer;
  int _reconnectAttempts = 0;
  bool _explicitlyDisconnected = false;
  int _lastIngestUtcNs = 0;

  ExchangeAdapter(this.name, this._tradeController, this._httpClient, this._log,
      this._sendMessage);

  String get websocketUrl;
  dynamic getSubscribeMessage(String normalizedSymbol);
  void handleWebSocketMessage(dynamic message, String normalizedSymbol);
  Future<List<Candle>> fetchHistoricalCandles(
      String symbol, String timeframe, DateTime start, DateTime end);

  String _getExchangeSymbol(String normalizedSymbol) {
    return _symbolNormalizationMap[name]?[normalizedSymbol] ??
        (throw Exception("Symbol $normalizedSymbol not supported on $name"));
  }

  Future<void> connectAndSubscribe(String normalizedSymbol) async {
    _explicitlyDisconnected = false;
    _log.info("[$name] Connecting and subscribing to $normalizedSymbol...");
    await _connect(normalizedSymbol);
  }

  Future<void> _connect(String normalizedSymbol) async {
    if (_ws != null || _explicitlyDisconnected) return;
    _updateStatus(false);

    try {
      _ws = IOWebSocketChannel.connect(Uri.parse(websocketUrl));
      _log.info("[$name] WebSocket connected.");
      _reconnectAttempts = 0;
      _updateStatus(true);

      _ws!.sink.add(jsonEncode(getSubscribeMessage(normalizedSymbol)));
      _log.info("[$name] Sent subscribe message for $normalizedSymbol.");

      _wsSubscription = _ws!.stream.listen(
        (message) {
          _lastIngestUtcNs =
              DateTime.now().toUtc().microsecondsSinceEpoch * 1000;
          handleWebSocketMessage(message, normalizedSymbol);
        },
        onDone: () {
          _log.warning("[$name] WebSocket connection closed. Reconnecting...");
          _handleDisconnect();
        },
        onError: (error) {
          _log.severe("[$name] WebSocket error: $error. Reconnecting...");
          _handleDisconnect();
        },
        cancelOnError: true,
      );
      _startHeartbeat();
    } catch (e) {
      _log.severe("[$name] WebSocket connection failed: $e. Retrying...");
      _handleDisconnect();
    }
  }

  void _handleDisconnect() {
    if (_explicitlyDisconnected) return;
    _stopHeartbeat();
    _wsSubscription?.cancel();
    _ws = null;
    _updateStatus(false);
    _scheduleReconnect();
  }

  void _scheduleReconnect() {
    _reconnectTimer?.cancel();
    if (_explicitlyDisconnected) return;

    final backoffSeconds = min(30.0, 0.5 * pow(2.0, _reconnectAttempts));
    final jitter = (Random().nextDouble() * 0.2 * backoffSeconds);
    final delay =
        Duration(milliseconds: ((backoffSeconds + jitter) * 1000).toInt());

    _reconnectAttempts++;
    _log.info(
        "[$name] Scheduling reconnect in ${delay.inSeconds} seconds (attempt $_reconnectAttempts).");
    _reconnectTimer = Timer(delay,
        () => connectAndSubscribe(_symbolNormalizationMap[name]!.keys.first));
  }

  Future<void> disconnect() async {
    _log.info("[$name] Disconnecting...");
    _explicitlyDisconnected = true;
    _reconnectTimer?.cancel();
    _stopHeartbeat();
    await _wsSubscription?.cancel();
    await _ws?.sink.close();
    _ws = null;
    _updateStatus(false);
  }

  void _startHeartbeat();
  void _stopHeartbeat() {
    _heartbeatTimer?.cancel();
    _heartbeatTimer = null;
  }

  void _updateStatus(bool connected) {
    _sendMessage('status', {
      'exchange': name,
      'connected': connected,
      'lastIngestUtcNs': _lastIngestUtcNs,
      'latencyMsEstimate': 0, // Placeholder
    });
  }
}

// --- CONCRETE ADAPTER IMPLEMENTATIONS ---

class BinanceAdapter extends ExchangeAdapter {
  BinanceAdapter(
      super.tradeController, super.httpClient, super.log, super.sendMessage)
      : super('Binance');

  @override
  String get websocketUrl =>
      'wss://stream.binance.com:9443/ws/${_getExchangeSymbol("BTC/USDT").toLowerCase()}@trade';

  @override
  dynamic getSubscribeMessage(String normalizedSymbol) =>
      {}; // Subscription is in URL

  @override
  void handleWebSocketMessage(dynamic message, String normalizedSymbol) {
    final data = jsonDecode(message);
    if (data['e'] == 'trade') {
      final trade = NormalizedTrade(
        symbol: normalizedSymbol,
        venue: name,
        priceInt: _stringToInt(data['p']),
        sizeInt: _stringToInt(data['q']),
        timestampUtcNs: (data['T'] as int) * 1000000,
      );
      _tradeController.add(trade);
    }
  }

  @override
  void _startHeartbeat() {
    // Binance uses implicit heartbeats (ping/pong handled by library)
  }

  @override
  Future<List<Candle>> fetchHistoricalCandles(
      String symbol, String timeframe, DateTime start, DateTime end) async {
    final intervalMap = {
      "1m": "1m",
      "5m": "5m",
      "15m": "15m",
      "30m": "30m",
      "1h": "1h",
      "4h": "4h",
      "1d": "1d",
      "1w": "1w"
    };
    final interval = intervalMap[timeframe]!;
    final exchangeSymbol = _getExchangeSymbol(symbol);
    final url =
        'https://api.binance.com/api/v3/klines?symbol=${exchangeSymbol}&interval=${interval}&startTime=${start.millisecondsSinceEpoch}&limit=1000';

    final response = await _httpClient.get(Uri.parse(url));
    if (response.statusCode != 200)
      throw Exception('Failed to load Binance candles: ${response.body}');

    final List<dynamic> data = jsonDecode(response.body);
    return data
        .map((kline) => Candle(
              symbol: symbol,
              timeframe: timeframe,
              openTimeUtcS: (kline[0] as int) ~/ 1000,
              openInt: _stringToInt(kline[1]),
              highInt: _stringToInt(kline[2]),
              lowInt: _stringToInt(kline[3]),
              closeInt: _stringToInt(kline[4]),
              volumeInt: _stringToInt(kline[5]),
            ))
        .toList();
  }
}

class OKXAdapter extends ExchangeAdapter {
  OKXAdapter(
      super.tradeController, super.httpClient, super.log, super.sendMessage)
      : super('OKX');

  @override
  String get websocketUrl => 'wss://ws.okx.com:8443/ws/v5/public';

  @override
  dynamic getSubscribeMessage(String normalizedSymbol) => {
        "op": "subscribe",
        "args": [
          {"channel": "trades", "instId": _getExchangeSymbol(normalizedSymbol)}
        ]
      };

  @override
  void handleWebSocketMessage(dynamic message, String normalizedSymbol) {
    final data = jsonDecode(message);
    if (data['arg']?['channel'] == 'trades') {
      for (var tradeData in data['data']) {
        final trade = NormalizedTrade(
          symbol: normalizedSymbol,
          venue: name,
          priceInt: _stringToInt(tradeData['px']),
          sizeInt: _stringToInt(tradeData['sz']),
          timestampUtcNs: int.parse(tradeData['ts']) * 1000000,
        );
        _tradeController.add(trade);
      }
    }
  }

  @override
  void _startHeartbeat() {
    _heartbeatTimer = Timer.periodic(const Duration(seconds: 15), (timer) {
      _ws?.sink.add('ping');
    });
  }

  @override
  Future<List<Candle>> fetchHistoricalCandles(
      String symbol, String timeframe, DateTime start, DateTime end) async {
    // OKX API is complex for historical range, this is a simplified implementation
    final intervalMap = {
      "1m": "1m",
      "5m": "5m",
      "15m": "15m",
      "30m": "30m",
      "1h": "1H",
      "4h": "4H",
      "1d": "1D",
      "1w": "1W"
    };
    final bar = intervalMap[timeframe]!;
    final exchangeSymbol = _getExchangeSymbol(symbol);
    final url =
        'https://www.okx.com/api/v5/market/candles?instId=${exchangeSymbol}&bar=${bar}&limit=300';

    final response = await _httpClient.get(Uri.parse(url));
    if (response.statusCode != 200)
      throw Exception('Failed to load OKX candles: ${response.body}');

    final List<dynamic> data = jsonDecode(response.body)['data'];
    return data
        .map((kline) => Candle(
              symbol: symbol,
              timeframe: timeframe,
              openTimeUtcS: int.parse(kline[0]) ~/ 1000,
              openInt: _stringToInt(kline[1]),
              highInt: _stringToInt(kline[2]),
              lowInt: _stringToInt(kline[3]),
              closeInt: _stringToInt(kline[4]),
              volumeInt: _stringToInt(kline[5]),
            ))
        .toList();
  }
}

class BitgetAdapter extends ExchangeAdapter {
  BitgetAdapter(
      super.tradeController, super.httpClient, super.log, super.sendMessage)
      : super('Bitget');

  @override
  String get websocketUrl => 'wss://ws.bitget.com/v2/spot/public';

  @override
  dynamic getSubscribeMessage(String normalizedSymbol) => {
        "op": "subscribe",
        "args": [
          {
            "instType": "SPOT",
            "channel": "trade",
            "instId": _getExchangeSymbol(normalizedSymbol)
          }
        ]
      };

  @override
  void handleWebSocketMessage(dynamic message, String normalizedSymbol) {
    final data = jsonDecode(message);
    if (data['action'] == 'snapshot' || data['action'] == 'update') {
      for (var tradeData in data['data']) {
        final isCompact = tradeData is List;
        final trade = NormalizedTrade(
          symbol: normalizedSymbol,
          venue: name,
          priceInt: _stringToInt(isCompact ? tradeData[1] : tradeData['p']),
          sizeInt: _stringToInt(isCompact ? tradeData[2] : tradeData['q']),
          timestampUtcNs: (isCompact
                  ? int.parse(tradeData[0])
                  : int.parse(tradeData['t'])) *
              1000000,
        );
        _tradeController.add(trade);
      }
    }
  }

  @override
  void _startHeartbeat() {
    _heartbeatTimer = Timer.periodic(const Duration(seconds: 15), (timer) {
      _ws?.sink.add('ping');
    });
  }

  @override
  Future<List<Candle>> fetchHistoricalCandles(
      String symbol, String timeframe, DateTime start, DateTime end) async {
    final granularity = _timeframeSeconds[timeframe]!;
    final exchangeSymbol = _getExchangeSymbol(symbol);
    final url =
        'https://api.bitget.com/api/spot/v1/market/candles?symbol=${exchangeSymbol}&granularity=${granularity}&limit=1000';

    final response = await _httpClient.get(Uri.parse(url));
    if (response.statusCode != 200)
      throw Exception('Failed to load Bitget candles: ${response.body}');

    final List<dynamic> data = jsonDecode(response.body);
    return data
        .map((kline) => Candle(
              symbol: symbol,
              timeframe: timeframe,
              openTimeUtcS: int.parse(kline[0]) ~/ 1000,
              openInt: _stringToInt(kline[1]),
              highInt: _stringToInt(kline[2]),
              lowInt: _stringToInt(kline[3]),
              closeInt: _stringToInt(kline[4]),
              volumeInt: _stringToInt(kline[5]),
            ))
        .toList();
  }
}

class CoinbaseAdapter extends ExchangeAdapter {
  CoinbaseAdapter(
      super.tradeController, super.httpClient, super.log, super.sendMessage)
      : super('Coinbase Exchange');

  @override
  String get websocketUrl => 'wss://ws-feed.exchange.coinbase.com';

  @override
  dynamic getSubscribeMessage(String normalizedSymbol) => {
        "type": "subscribe",
        "product_ids": [_getExchangeSymbol(normalizedSymbol)],
        "channels": ["matches"]
      };

  @override
  void handleWebSocketMessage(dynamic message, String normalizedSymbol) {
    final data = jsonDecode(message);
    if (data['type'] == 'match') {
      final trade = NormalizedTrade(
        symbol: normalizedSymbol,
        venue: name,
        priceInt: _stringToInt(data['price']),
        sizeInt: _stringToInt(data['size']),
        timestampUtcNs:
            DateTime.parse(data['time']).microsecondsSinceEpoch * 1000,
      );
      _tradeController.add(trade);
    }
  }

  @override
  void _startHeartbeat() {/* Coinbase does not require client-side pings */}

  @override
  Future<List<Candle>> fetchHistoricalCandles(
      String symbol, String timeframe, DateTime start, DateTime end) async {
    // Coinbase has limited granularities, so we might need to aggregate
    final supportedSeconds = [60, 300, 900, 3600, 21600, 86400];
    final requestedSeconds = _timeframeSeconds[timeframe]!;

    int fetchGranularity = supportedSeconds.lastWhere(
        (s) => s <= requestedSeconds,
        orElse: () => supportedSeconds.first);

    final exchangeSymbol = _getExchangeSymbol(symbol);
    final url =
        'https://api.exchange.coinbase.com/products/$exchangeSymbol/candles?granularity=$fetchGranularity&start=${start.toIso8601String()}&end=${end.toIso8601String()}';

    final response = await _httpClient.get(Uri.parse(url));
    if (response.statusCode != 200)
      throw Exception('Failed to load Coinbase candles: ${response.body}');

    final List<dynamic> data = jsonDecode(response.body);
    List<Candle> fetchedCandles = data
        .map((kline) => Candle(
              symbol: symbol,
              timeframe: timeframe, // Temporarily use target timeframe
              openTimeUtcS: kline[0] as int,
              lowInt: _doubleToInt(kline[1].toDouble()),
              highInt: _doubleToInt(kline[2].toDouble()),
              openInt: _doubleToInt(kline[3].toDouble()),
              closeInt: _doubleToInt(kline[4].toDouble()),
              volumeInt: _doubleToInt(kline[5].toDouble()),
            ))
        .toList()
        .reversed
        .toList(); // API returns newest first

    if (fetchGranularity == requestedSeconds) {
      return fetchedCandles;
    } else {
      return _aggregateCandles(
          fetchedCandles, symbol, timeframe, requestedSeconds);
    }
  }
}

class BitstampAdapter extends ExchangeAdapter {
  BitstampAdapter(
      super.tradeController, super.httpClient, super.log, super.sendMessage)
      : super('Bitstamp');

  @override
  String get websocketUrl => 'wss://ws.bitstamp.net';

  @override
  dynamic getSubscribeMessage(String normalizedSymbol) => {
        "event": "bts:subscribe",
        "data": {
          "channel": "live_trades_${_getExchangeSymbol(normalizedSymbol)}"
        }
      };

  @override
  void handleWebSocketMessage(dynamic message, String normalizedSymbol) {
    final msg = jsonDecode(message);
    if (msg['event'] == 'trade') {
      final data = msg['data'];
      final trade = NormalizedTrade(
        symbol: normalizedSymbol,
        venue: name,
        priceInt: _stringToInt(data['price'].toString()),
        sizeInt: _stringToInt(data['amount'].toString()),
        timestampUtcNs: int.parse(data['timestamp']) * 1000000000,
      );
      _tradeController.add(trade);
    }
  }

  @override
  void _startHeartbeat() {
    _heartbeatTimer = Timer.periodic(const Duration(seconds: 15), (timer) {
      _ws?.sink.add(jsonEncode({"event": "bts:heartbeat"}));
    });
  }

  @override
  Future<List<Candle>> fetchHistoricalCandles(
      String symbol, String timeframe, DateTime start, DateTime end) async {
    final step = _timeframeSeconds[timeframe]!;
    final exchangeSymbol = _getExchangeSymbol(symbol);
    final url =
        'https://www.bitstamp.net/api/v2/ohlc/$exchangeSymbol/?step=$step&limit=1000&start=${start.millisecondsSinceEpoch ~/ 1000}';

    final response = await _httpClient.get(Uri.parse(url));
    if (response.statusCode != 200)
      throw Exception('Failed to load Bitstamp candles: ${response.body}');

    final List<dynamic> data = jsonDecode(response.body)['data']['ohlc'];
    return data
        .map((kline) => Candle(
              symbol: symbol,
              timeframe: timeframe,
              openTimeUtcS: int.parse(kline['timestamp']),
              openInt: _stringToInt(kline['open']),
              highInt: _stringToInt(kline['high']),
              lowInt: _stringToInt(kline['low']),
              closeInt: _stringToInt(kline['close']),
              volumeInt: _stringToInt(kline['volume']),
            ))
        .toList();
  }
}

class KrakenAdapter extends ExchangeAdapter {
  KrakenAdapter(
      super.tradeController, super.httpClient, super.log, super.sendMessage)
      : super('Kraken');

  @override
  String get websocketUrl => 'wss://ws.kraken.com';

  @override
  dynamic getSubscribeMessage(String normalizedSymbol) => {
        "event": "subscribe",
        "pair": [normalizedSymbol], // Kraken uses normalized symbol directly
        "subscription": {"name": "trade"}
      };

  @override
  void handleWebSocketMessage(dynamic message, String normalizedSymbol) {
    final data = jsonDecode(message);
    if (data is List && data.length > 2 && data[2] == 'trade') {
      final pair = data[3];
      for (var tradeData in data[1]) {
        final trade = NormalizedTrade(
          symbol: pair,
          venue: name,
          priceInt: _stringToInt(tradeData[0]),
          sizeInt: _stringToInt(tradeData[1]),
          timestampUtcNs: (double.parse(tradeData[2]) * 1e9).toInt(),
        );
        _tradeController.add(trade);
      }
    }
  }

  @override
  void _startHeartbeat() {
    _heartbeatTimer = Timer.periodic(const Duration(seconds: 15), (timer) {
      _ws?.sink.add(jsonEncode({"event": "ping"}));
    });
  }

  @override
  Future<List<Candle>> fetchHistoricalCandles(
      String symbol, String timeframe, DateTime start, DateTime end) async {
    final pairMap = {"BTC/USD": "XXBTZUSD", "BTC/EUR": "XXBTZEUR"};
    final intervalMap = {
      "1m": 1,
      "5m": 5,
      "15m": 15,
      "30m": 30,
      "1h": 60,
      "4h": 240,
      "1d": 1440,
      "1w": 10080
    };
    final interval = intervalMap[timeframe]!;
    final pair = pairMap[symbol]!;
    final url =
        'https://api.kraken.com/0/public/OHLC?pair=$pair&interval=$interval&since=${start.millisecondsSinceEpoch ~/ 1000}';

    final response = await _httpClient.get(Uri.parse(url));
    if (response.statusCode != 200)
      throw Exception('Failed to load Kraken candles: ${response.body}');

    final Map<String, dynamic> data = jsonDecode(response.body);
    final List<dynamic> klines = data['result'][pair];
    return klines
        .map((kline) => Candle(
              symbol: symbol,
              timeframe: timeframe,
              openTimeUtcS: kline[0] as int,
              openInt: _stringToInt(kline[1]),
              highInt: _stringToInt(kline[2]),
              lowInt: _stringToInt(kline[3]),
              closeInt: _stringToInt(kline[4]),
              volumeInt: _stringToInt(kline[6]),
            ))
        .toList();
  }
}

class BitvavoAdapter extends ExchangeAdapter {
  BitvavoAdapter(
      super.tradeController, super.httpClient, super.log, super.sendMessage)
      : super('Bitvavo');

  @override
  String get websocketUrl => 'wss://ws.bitvavo.com/v2/';

  @override
  dynamic getSubscribeMessage(String normalizedSymbol) => {
        "action": "subscribe",
        "channels": [
          {
            "name": "trades",
            "markets": [_getExchangeSymbol(normalizedSymbol)]
          }
        ]
      };

  @override
  void handleWebSocketMessage(dynamic message, String normalizedSymbol) {
    final data = jsonDecode(message);
    if (data['event'] == 'trade') {
      final trade = NormalizedTrade(
        symbol: normalizedSymbol,
        venue: name,
        priceInt: _stringToInt(data['price']),
        sizeInt: _stringToInt(data['amount']),
        timestampUtcNs: data['timestamp'] as int,
      );
      _tradeController.add(trade);
    }
  }

  @override
  void _startHeartbeat() {/* Bitvavo does not require client-side pings */}

  @override
  Future<List<Candle>> fetchHistoricalCandles(
      String symbol, String timeframe, DateTime start, DateTime end) async {
    final intervalMap = {
      "1m": "1m",
      "5m": "5m",
      "15m": "15m",
      "30m": "30m",
      "1h": "1h",
      "4h": "4h",
      "1d": "1d",
      "1w": "1w"
    };
    final interval = intervalMap[timeframe]!;
    final exchangeSymbol = _getExchangeSymbol(symbol);
    final url =
        'https://api.bitvavo.com/v2/$exchangeSymbol/candles?interval=$interval&start=${start.millisecondsSinceEpoch}';

    final response = await _httpClient.get(Uri.parse(url));
    if (response.statusCode != 200)
      throw Exception('Failed to load Bitvavo candles: ${response.body}');

    final List<dynamic> data = jsonDecode(response.body);
    return data
        .map((kline) => Candle(
              symbol: symbol,
              timeframe: timeframe,
              openTimeUtcS: kline[0] ~/ 1000,
              openInt: _stringToInt(kline[1]),
              highInt: _stringToInt(kline[2]),
              lowInt: _stringToInt(kline[3]),
              closeInt: _stringToInt(kline[4]),
              volumeInt: _stringToInt(kline[5]),
            ))
        .toList();
  }
}

// --- HELPER FOR CANDLE AGGREGATION (TIMEFRAME FALLBACK) ---
List<Candle> _aggregateCandles(List<Candle> smallerCandles, String symbol,
    String targetTimeframe, int targetSeconds) {
  if (smallerCandles.isEmpty) return [];

  final List<Candle> aggregated = [];

  int currentBucketStart =
      (smallerCandles.first.openTimeUtcS ~/ targetSeconds) * targetSeconds;
  int openInt = smallerCandles.first.openInt;
  int highInt = smallerCandles.first.highInt;
  int lowInt = smallerCandles.first.lowInt;
  int closeInt = smallerCandles.first.closeInt;
  int volumeInt = smallerCandles.first.volumeInt;

  for (int i = 1; i < smallerCandles.length; i++) {
    final candle = smallerCandles[i];
    final bucketStart = (candle.openTimeUtcS ~/ targetSeconds) * targetSeconds;

    if (bucketStart == currentBucketStart) {
      highInt = max(highInt, candle.highInt);
      lowInt = min(lowInt, candle.lowInt);
      closeInt = candle.closeInt;
      volumeInt += candle.volumeInt;
    } else {
      aggregated.add(Candle(
        symbol: symbol,
        timeframe: targetTimeframe,
        openTimeUtcS: currentBucketStart,
        openInt: openInt,
        highInt: highInt,
        lowInt: lowInt,
        closeInt: closeInt,
        volumeInt: volumeInt,
      ));

      currentBucketStart = bucketStart;
      openInt = candle.openInt;
      highInt = candle.highInt;
      lowInt = candle.lowInt;
      closeInt = candle.closeInt;
      volumeInt = candle.volumeInt;
    }
  }

  aggregated.add(Candle(
    symbol: symbol,
    timeframe: targetTimeframe,
    openTimeUtcS: currentBucketStart,
    openInt: openInt,
    highInt: highInt,
    lowInt: lowInt,
    closeInt: closeInt,
    volumeInt: volumeInt,
  ));

  return aggregated;
}
