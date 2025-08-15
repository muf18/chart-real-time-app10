// file_path: lib/main_desktop.dart
import 'dart:async';
import 'dart:convert';
import 'dart:isolate';
import 'dart:math';

import 'package:candlesticks/candlesticks.dart' as cs;
import 'package:decimal/decimal.dart';
import 'package:fl_chart/fl_chart.dart';
import 'package:flutter/material.dart';
import 'package:path_provider/path_provider.dart';

// Import the core logic. This is a placeholder for the isolate entry point.
import 'app_core.dart' as core;

// --- DATA MODELS (UI specific, mirrors core for type safety) ---
// These models are used only for deserializing data from the core isolate.
// They are defined here to avoid Flutter dependencies in app_core.dart.

class AggregatedDataPoint {
  final String symbol;
  final String timeframe;
  final int timestampUtcS;
  final int vwapInt;
  final int volumeInt;
  final int lastPriceInt;
  final bool amend;

  AggregatedDataPoint.fromJson(Map<String, dynamic> json)
      : symbol = json['symbol'],
        timeframe = json['timeframe'],
        timestampUtcS = json['timestampUtcS'],
        vwapInt = json['vwapInt'],
        volumeInt = json['volumeInt'],
        lastPriceInt = json['lastPriceInt'],
        amend = json['amend'];
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

  Candle.fromJson(Map<String, dynamic> json)
      : symbol = json['symbol'],
        timeframe = json['timeframe'],
        openTimeUtcS = json['openTimeUtcS'],
        openInt = json['openInt'],
        highInt = json['highInt'],
        lowInt = json['lowInt'],
        closeInt = json['closeInt'],
        volumeInt = json['volumeInt'];
}

// --- ISOLATE BRIDGE ---
// This class is the SOLE interface between the Flutter UI and the core Isolate.
class CoreIsolateBridge {
  Isolate? _isolate;
  SendPort? _sendPort;
  final ReceivePort _receivePort = ReceivePort();
  int _reqCounter = 0;

  final _aggregatedDataPointController =
      StreamController<AggregatedDataPoint>.broadcast();
  final _candleHistoryController = StreamController<List<Candle>>.broadcast();
  final _connectionStatusController =
      StreamController<Map<String, dynamic>>.broadcast();
  final _errorController = StreamController<Map<String, dynamic>>.broadcast();

  Stream<AggregatedDataPoint> get aggregatedDataPointStream =>
      _aggregatedDataPointController.stream;
  Stream<List<Candle>> get candleHistoryStream =>
      _candleHistoryController.stream;
  Stream<Map<String, dynamic>> get connectionStatusStream =>
      _connectionStatusController.stream;
  Stream<Map<String, dynamic>> get errorStream => _errorController.stream;

  Future<void> initialize(String stateDirPath) async {
    _isolate = await Isolate.spawn(core.coreIsolateMain, _receivePort.sendPort);

    final Completer<void> completer = Completer();

    _receivePort.listen((message) {
      if (message is SendPort) {
        _sendPort = message;
        postCommand({
          'type': 'init',
          'stateDirPath': stateDirPath,
          'debug': true,
        });
        completer.complete();
      } else {
        _handleMessageFromCore(message);
      }
    });

    return completer.future;
  }

  void _handleMessageFromCore(dynamic message) {
    try {
      final Map<String, dynamic> decoded = jsonDecode(message);
      final type = decoded['type'] as String;
      final data = decoded['data'];

      switch (type) {
        case 'aggregated':
          _aggregatedDataPointController
              .add(AggregatedDataPoint.fromJson(data));
          break;
        case 'candle':
          // For simplicity, we'll handle single candles as a list of one.
          // A real implementation might batch these.
          _candleHistoryController.add([Candle.fromJson(data)]);
          break;
        case 'status':
          _connectionStatusController.add(data);
          break;
        case 'error':
          _errorController.add(data);
          break;
        case 'ack':
          // Can be used with Completers for request-response patterns
          break;
      }
    } catch (e) {
      debugPrint("UI: Failed to handle message from core: $e");
    }
  }

  void postCommand(Map<String, dynamic> command) {
    if (_sendPort != null) {
      command['req_id'] = 'req-${_reqCounter++}';
      _sendPort!.send(jsonEncode(command));
    } else {
      debugPrint("UI: Core isolate not ready, command dropped.");
    }
  }

  void dispose() {
    postCommand({'type': 'shutdown'});
    _receivePort.close();
    _isolate?.kill(priority: Isolate.immediate);
    _isolate = null;
    _aggregatedDataPointController.close();
    _candleHistoryController.close();
    _connectionStatusController.close();
    _errorController.close();
  }
}

// --- MAIN APPLICATION ---
void main() async {
  WidgetsFlutterBinding.ensureInitialized();
  final stateDirPath = (await getApplicationSupportDirectory()).path;
  final coreBridge = CoreIsolateBridge();
  await coreBridge.initialize(stateDirPath);
  runApp(MyApp(coreBridge: coreBridge));
}

class MyApp extends StatelessWidget {
  final CoreIsolateBridge coreBridge;
  const MyApp({super.key, required this.coreBridge});

  @override
  Widget build(BuildContext context) {
    return MaterialApp(
      title: 'Crypto Chart Client (Desktop)',
      theme: ThemeData.dark(),
      home: MyHomePage(coreBridge: coreBridge),
    );
  }
}

class MyHomePage extends StatefulWidget {
  final CoreIsolateBridge coreBridge;
  const MyHomePage({super.key, required this.coreBridge});

  @override
  State<MyHomePage> createState() => _MyHomePageState();
}

class _MyHomePageState extends State<MyHomePage> {
  final ValueNotifier<String> _selectedSymbol = ValueNotifier('BTC/USDT');
  final ValueNotifier<String> _selectedTimeframe = ValueNotifier('1m');
  final List<String> _supportedSymbols = ['BTC/USDT', 'BTC/USD', 'BTC/EUR'];
  final List<String> _supportedTimeframes = [
    "1m",
    "5m",
    "15m",
    "30m",
    "1h",
    "4h",
    "1d",
    "1w"
  ];

  final List<cs.Candle> _candles = [];
  final List<FlSpot> _vwapData = [];
  final List<BarChartGroupData> _volumeData = [];

  StreamSubscription? _candleSubscription;
  StreamSubscription? _aggSubscription;
  StreamSubscription? _errorSubscription;

  @override
  void initState() {
    super.initState();
    _listenToCoreStreams();
    _initialBackfill();

    _errorSubscription = widget.coreBridge.errorStream.listen((error) {
      if (mounted) {
        ScaffoldMessenger.of(context).showSnackBar(
          SnackBar(
            content: Text('Core Error: ${error['code']} - ${error['message']}'),
            backgroundColor: Colors.redAccent,
          ),
        );
      }
    });
  }

  void _listenToCoreStreams() {
    _candleSubscription?.cancel();
    _candleSubscription =
        widget.coreBridge.candleHistoryStream.listen((newCandles) {
      setState(() {
        for (var c in newCandles) {
          _candles.add(_toCsCandle(c));
        }
        _candles.sort((a, b) => a.date.compareTo(b.date));
      });
    });

    _aggSubscription?.cancel();
    _aggSubscription =
        widget.coreBridge.aggregatedDataPointStream.listen((agg) {
      if (agg.symbol == _selectedSymbol.value &&
          agg.timeframe == _selectedTimeframe.value) {
        setState(() {
          final timestampMs = agg.timestampUtcS * 1000.0;
          final vwap = _intToDouble(agg.vwapInt);
          final volume = _intToDouble(agg.volumeInt);

          final vwapSpot = FlSpot(timestampMs, vwap);
          final volumeBar = BarChartGroupData(
            x: agg.timestampUtcS,
            barRods: [BarChartRodData(toY: volume, color: Colors.blueGrey)],
          );

          if (agg.amend) {
            if (_vwapData.isNotEmpty) _vwapData.removeLast();
            if (_volumeData.isNotEmpty) _volumeData.removeLast();
          }
          _vwapData.add(vwapSpot);
          _volumeData.add(volumeBar);

          // Update last candle
          if (_candles.isNotEmpty &&
              _candles.last.date.millisecondsSinceEpoch ~/ 1000 ==
                  agg.timestampUtcS) {
            final last = _candles.last;
            _candles.last = cs.Candle(
              date: last.date,
              open: last.open,
              high: max(last.high, _intToDouble(agg.lastPriceInt)),
              low: min(last.low, _intToDouble(agg.lastPriceInt)),
              close: _intToDouble(agg.lastPriceInt),
              volume: volume,
            );
          } else {
            final newCandle = cs.Candle(
              date:
                  DateTime.fromMillisecondsSinceEpoch(agg.timestampUtcS * 1000),
              open: _intToDouble(agg.lastPriceInt),
              high: _intToDouble(agg.lastPriceInt),
              low: _intToDouble(agg.lastPriceInt),
              close: _intToDouble(agg.lastPriceInt),
              volume: volume,
            );
            _candles.add(newCandle);
          }
        });
      }
    });
  }

  void _initialBackfill() {
    final now = DateTime.now().toUtc();
    final start = now.subtract(const Duration(days: 7));
    widget.coreBridge.postCommand({
      'type': 'backfill',
      'symbol': _selectedSymbol.value,
      'timeframe': _selectedTimeframe.value,
      'startIso': start.toIso8601String(),
      'endIso': now.toIso8601String(),
    });
  }

  void _resetAndFetchData() {
    setState(() {
      _candles.clear();
      _vwapData.clear();
      _volumeData.clear();
    });
    _initialBackfill();
  }

  @override
  void dispose() {
    _candleSubscription?.cancel();
    _aggSubscription?.cancel();
    _errorSubscription?.cancel();
    widget.coreBridge.dispose();
    _selectedSymbol.dispose();
    _selectedTimeframe.dispose();
    super.dispose();
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(
        title: const Text('Crypto Chart Client (Desktop)'),
      ),
      body: Column(
        children: [
          _buildControlPanel(),
          Expanded(child: _buildChart()),
          _buildStatusBar(),
        ],
      ),
    );
  }

  Widget _buildControlPanel() {
    return Padding(
      padding: const EdgeInsets.all(8.0),
      child: Row(
        children: [
          const Text("Symbol: "),
          ValueListenableBuilder<String>(
            valueListenable: _selectedSymbol,
            builder: (context, value, child) {
              return DropdownButton<String>(
                value: value,
                items: _supportedSymbols
                    .map((s) => DropdownMenuItem(value: s, child: Text(s)))
                    .toList(),
                onChanged: (newValue) {
                  if (newValue != null) {
                    _selectedSymbol.value = newValue;
                    widget.coreBridge
                        .postCommand({'type': 'setSymbol', 'symbol': newValue});
                    _resetAndFetchData();
                  }
                },
              );
            },
          ),
          const SizedBox(width: 20),
          const Text("Timeframe: "),
          ValueListenableBuilder<String>(
            valueListenable: _selectedTimeframe,
            builder: (context, value, child) {
              return DropdownButton<String>(
                value: value,
                items: _supportedTimeframes
                    .map((t) => DropdownMenuItem(value: t, child: Text(t)))
                    .toList(),
                onChanged: (newValue) {
                  if (newValue != null) {
                    _selectedTimeframe.value = newValue;
                    widget.coreBridge.postCommand(
                        {'type': 'setTimeframe', 'timeframe': newValue});
                    _resetAndFetchData();
                  }
                },
              );
            },
          ),
        ],
      ),
    );
  }

  Widget _buildChart() {
    return _candles.isEmpty
        ? const Center(child: CircularProgressIndicator())
        : Column(
            children: [
              Expanded(
                flex: 3,
                child: cs.Candlesticks(
                  candles: _candles,
                  actions: [
                    cs.ToolBarAction(
                      onPressed: () {},
                      child: const Icon(Icons.zoom_in),
                    ),
                    cs.ToolBarAction(
                      onPressed: () {},
                      child: const Icon(Icons.zoom_out),
                    ),
                  ],
                  overlayInfo: (candle) => {
                    "VWAP": _getVwapForCandle(candle).toStringAsFixed(2),
                  },
                ),
              ),
              Expanded(
                flex: 1,
                child: Padding(
                  padding: const EdgeInsets.only(right: 16.0, top: 8.0),
                  child: BarChart(
                    BarChartData(
                      barGroups: _volumeData,
                      titlesData: const FlTitlesData(show: false),
                      borderData: FlBorderData(show: false),
                      gridData: const FlGridData(show: false),
                    ),
                  ),
                ),
              ),
            ],
          );
  }

  double _getVwapForCandle(cs.Candle? candle) {
    if (candle == null) return 0.0;
    final timestampMs = candle.date.millisecondsSinceEpoch.toDouble();
    final matchingVwap = _vwapData.where((spot) => spot.x == timestampMs);
    return matchingVwap.isNotEmpty ? matchingVwap.first.y : 0.0;
  }

  Widget _buildStatusBar() {
    return StreamBuilder<Map<String, dynamic>>(
      stream: widget.coreBridge.connectionStatusStream,
      builder: (context, snapshot) {
        if (!snapshot.hasData) {
          return const SizedBox.shrink();
        }
        final status = snapshot.data!;
        final exchange = status['exchange'];
        final isConnected = status['connected'] as bool;
        return Container(
          padding: const EdgeInsets.symmetric(horizontal: 8.0, vertical: 4.0),
          color: Colors.blueGrey.shade900,
          child: Row(
            children: [
              Text('$exchange: '),
              Icon(
                Icons.circle,
                color: isConnected ? Colors.green : Colors.red,
                size: 12,
              ),
              const SizedBox(width: 4),
              Text(isConnected ? 'Connected' : 'Disconnected'),
            ],
          ),
        );
      },
    );
  }

  cs.Candle _toCsCandle(Candle c) {
    return cs.Candle(
      date: DateTime.fromMillisecondsSinceEpoch(c.openTimeUtcS * 1000),
      open: _intToDouble(c.openInt),
      high: _intToDouble(c.highInt),
      low: _intToDouble(c.lowInt),
      close: _intToDouble(c.closeInt),
      volume: _intToDouble(c.volumeInt),
    );
  }

  double _intToDouble(int val) {
    return (Decimal.fromInt(val) / Decimal.fromInt(100000000)).toDouble();
  }
}
