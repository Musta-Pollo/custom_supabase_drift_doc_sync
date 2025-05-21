# Custom Drift Synchronization Example

This project showcases two things:
- Synchronization of one-user data across devices that are stored using Drift DB
     - Client side: A lot of the boilerplate is achieved through code generation
     - Server side: A generic code that can be easily reused as it is
- Synchronization of appflowy editor content across devices with handling of merge conflicts and offline editing using appflowy_editor_sync_plugin.


## Demo

Example of how this works in the browser. But the same applies for native platforms. The browser only has some workarounds viz: `database.dart`, `tab_separated_shared_preferences.dart`:

Link: https://habitmaster-e52e9.web.app/


https://github.com/user-attachments/assets/274e50de-ac70-42b1-9a43-083a8f0db7ac


**Notes:**
The demo is slightly longer because it is a live demonstration and involves turning the Wi-Fi on and off. This same demo functions well on all other Flutter platforms and Wear OS if properly configured.

Its mainly designed for native platforms. On the Web there is a problem that if user opens multiple tabs, the database needs to be unique for each tab. Because of that the web initialily syncs all data each time it is opened.

## The synchronization is achieved by:
### Client
- Annotation drift cases and providing special variables

```dart
@customSync // 1. Add customSync annotation
class Task extends Table {
  static String get serverTableName => "public.task"; // 2. specify server db table name with schema

  TextColumn get id => text().clientDefault(() => const Uuid().v4())();
  @JsonKey('created_at') // 3. Provide timestamp attributes a
  DateTimeColumn get createdAt => dateTime()();
  @JsonKey('updated_at') // 3. Provide timestamp attributes b
  DateTimeColumn get updatedAt => dateTime()();
  @JsonKey('deleted_at') // 3. Provide timestamp attributes c
  DateTimeColumn get deletedAt => dateTime().nullable()();
  TextColumn get name => text()();
  

  // 4. Specify the isRemote attribute
  BoolColumn get isRemote => boolean().withDefault(const Constant(false))();

  @JsonKey('project_id')
  TextColumn get projectId => text()();
  @JsonKey('user_id')
  TextColumn get userId => text()();

  @override
  Set<Column> get primaryKey => {id};
}
```

Define `SyncManager`
- `SyncManager` combines data from annotated classes and generates code that can be used with  `WatermelonDB` styled server DB functions.
```dart
part 'sync_manager.sync.dart';
// Specify tables to sync in - IN THE ORDER OF THEIR DEPENDENCIES
@SyncManager(classes: [Task, Project, Docup])
class SyncClass {
  const SyncClass();

  Future<void> sync(Map<String, dynamic> changes, AppDatabase db) =>
      _$SyncClassSync(changes, db);
  Future<Map<String, dynamic>> getChanges(
          DateTime lastSyncedAt, AppDatabase db, String currentInstanceId) =>
      _$SyncClassGetChanges(lastSyncedAt, db, currentInstanceId);
  List<String> syncedTables() => _$SyncClassSyncedTables();

  Stream<List<dynamic>> getUpdateStreams(AppDatabase db) =>
      _$SyncClassCombinedStreams(db);
}
```

But provide the tables in the order of dependencies as this will determine the order in which they will be inserted.

Then you can utilize the provided functions, for example like this to communicate with supabase:

```dart
class SyncManagerS {
  final AppDatabase db;
  final SupabaseClient supabase;
  final SharedPreferences basicSharePrefs;
  late final TabSeparateSharedPreferences sharedPrefs;
  bool _isSyncing = false;
  bool _isLoggedIn = false;
  final String _currentInstanceId = const Uuid().v4();
  bool _extraSyncNeeded = false;
  StreamSubscription? _streamSubscription;
  StreamSubscription<InternetStatus>? _connectionSubscription;

  SyncManagerS({
    required this.db,
    required this.supabase,
    required this.basicSharePrefs,
  }) : super() {
    _checkInitialLoginStatus();
    sharedPrefs = TabSeparateSharedPreferences.getInstance(basicSharePrefs);
  }

  Future<void> _checkInitialLoginStatus() async {
    // Replace this with your actual logic to check if the user is logged in.
    final session = supabase.auth.currentSession;
    if (session != null) {
      signIn();
    }
  }

  Future<void> _synchronize() async {
    await sequenceRetry(
      () => _synchronizeBase(),
    );
  }

  void _listenOnTheServerUpdates() {
    supabase.channel('db-changes').onAllSyncClassChanges(_currentInstanceId,
        (payload) {
      queueSyncDebounce();
    }).subscribe();
  }

  void _startListeningOnInternetChanges() {
  }

  void _startListening() {
    queueSync();
    _listenOnLocalUpdates();
    _listenOnTheServerUpdates();
    _startListeningOnInternetChanges();
  }

  void _listenOnLocalUpdates() {
    final combinedStream = const SyncClass().getUpdateStreams(db);
    _streamSubscription = combinedStream.listen((data) async {
      final localChanges = await const SyncClass().getChanges(
          _getLastPulledAt() ?? DateTime(2000), db, _currentInstanceId);
      final isLocalChangesEmpty = localChanges.values.every((element) {
        return (element as Map<String, dynamic>)
            .values
            .every((innerElement) => innerElement.isEmpty);
      });
      if (!isLocalChangesEmpty) {
        queueSyncDebounce();
      }
    });
  }

  void queueSyncDebounce() {
    EasyDebounce.debounce('sync', const Duration(milliseconds: 1000), () {
      E.t.debug('Debounce trigger sync');
      queueSync();
    });
  }

  Future<void> _synchronizeBase() async {
    final lastPulledAt = _getLastPulledAt() ?? DateTime(2000);
    final now = DateTime.now();

    // Pull changes from the server
    final pullResponse = await supabase.rpc('pull_changes', params: {
      'collections': const SyncClass().syncedTables(),
      'last_pulled_at': (lastPulledAt).toUtc().toIso8601String(),
    });

    final changes = pullResponse['changes'] as Map<String, dynamic>;

    // Apply changes to the local database
    await db.transaction(() async {
      await const SyncClass().sync(changes, db);
    });

    // Push local changes to the server
    final localChanges = await const SyncClass()
        .getChanges(lastPulledAt, db, _currentInstanceId);
    final isLocalChangesEmpty = localChanges.values.every((element) {
      return (element as Map<String, dynamic>)
          .values
          .every((innerElement) => innerElement.isEmpty);
    });
    if (!isLocalChangesEmpty) {
      try {
        final res = await supabase.rpc('push_changes', params: {
          '_changes': localChanges,
          'last_pulled_at': now.toIso8601String(),
        }).timeout(const Duration(seconds: 10));
        await _setLastPulledAt(DateTime.parse(res));
      } catch (e, st) {
        E.t.error(e, st);
        print('Push changes failed: $e');
      }
    } else {
      await _setLastPulledAt(now.subtract(const Duration(minutes: 2)));
    }

    //TODO: Delete synced deletes from local db
  }

  void signIn() {
    _isLoggedIn = true;
    _startListening();
  }

  void signOut() async {
    ...
  }

  void _stopListening() {
    ...
  }

  void queueSync() {
    if (_isSyncing) {
      E.t.debug('Sync already in progress');
      _extraSyncNeeded = true;
      return;
    }

    _isSyncing = true;
    E.t.debug('Sync started');
    _synchronize().then((_) {
      E.t.debug('Sync completed');
      _isSyncing = false;

      if (_extraSyncNeeded) {
        E.t.debug('Extra sync needed, scheduling first retry in 800ms');
        _extraSyncNeeded = false;
        ...
      }
    }).catchError((error, st) {
      _isSyncing = false;
      E.t.error(error, st);
    });
  }

  String lastPulledAtKey = 'lastPulledAt';

  /// Get lastPulledAt using SharedPreferences
  DateTime? _getLastPulledAt() {
    ...
  }

  Future<void> _setLastPulledAt(DateTime timestamp) async {
    ....
  }

  void dispose() {
    ...
}

```






### Server

On the server, you need to define two things:
- RLS rules such as this:
```sql
alter policy "doc_updates_rls"
on "public"."doc_updates"
to authenticated
using (
  (( SELECT auth.uid() AS uid) = user_id)
with check (
  (( SELEC
);
```

In Supabase, these rules are respected inside database functions, so simplify them.

- Database functions:
  - [push_changes](server/db_functions/push_changes.sql)
    - uses [helper functions](server/db_functions/push_changes_helpers.sql)
  - [pull_changes](server/db_functions/pull_changes.sql)


You can copy and paste them into your project if you synchronize data for just one user. The implementation is generic and utilizes RLS rules.


## Extra Attributes
Tables require special timestamp attributes for tracking changes:
**Client-side timestamp attributes:**
- created_at: Records when a record was created on the client
- updated_at: Records when a record was updated on the client
- deleted_at: Records when a record was deleted on the client

**Server-side timestamp attributes:**
- server_created_at: Records when a record was created on the server
- server_updated_at: Records when a record was updated on the server
- deleted_at: Records when a record was deleted on the server

An optional instance_id attribute enables more efficient filtering of real-time updates.

### Creating, Updating, Deleting on the Client

Whenever you create, update, delete and a you are assigning a new timestamp it is crucial to use: 
`DateTime.now().toUtc()` (it must be converted into a UTC timezone) and also to set `isRemote = false` . For example:

```dart
value.copyWith(
        name: newName,
        isRemote: false,
        updatedAt: DateTime.now().toUtc(),
      );
```
## Run this demo

On Supabase create a project and do these steps:
- Insert tables with RLS from `generate_db.sql`
- Add database functions from:
    - `pull_changes.sql`
    - `push_changes_helpers.sql`
    - `push_changes.sql`

Add to the project .env file with:
```
SUPABASE_URL=https://xxxxsupabase.co
SUPABASE_ANON_KEY=xxxxxx
SUPABASE_STORAGE_BUCKET=xxxx //Not used, but still provide it
```

Run the project :)

## Possible Extension for Multi-user use case
There is a problem that when the user can gain access or lose access to some resources, this timestamped synchronization would need extra tracking for changes that would require a complete resync.
This is not achieved in this project, but it would also be likely possible. 
