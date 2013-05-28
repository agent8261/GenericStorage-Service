package edu.umich.imlc.mydesk.test.db;

import java.util.ArrayList;
import java.util.List;

import edu.umich.imlc.mydesk.test.common.GenericContract.*;
import edu.umich.imlc.mydesk.test.common.Utils;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.DatabaseUtils;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.database.sqlite.SQLiteQueryBuilder;
import android.net.Uri;

public class GenericDb extends SQLiteOpenHelper
{
  static final String TAG = "GenericDb";
  private static final int DB_VERSION = 1;
  private static final String DB_NAME = "generic";
  private static final String CREATE_TABLE_METADATA = "CREATE TABLE "
      + Tables.METADATA + "(" + MetaDataColumns.ID + " INTEGER NOT NULL, "
      + MetaDataColumns.FILE_ID + " TEXT NOT NULL UNIQUE, "
      + MetaDataColumns.NAME + " TEXT NOT NULL, " + MetaDataColumns.TYPE
      + " INTEGER NOT NULL, " + MetaDataColumns.OWNER + " TEXT NOT NULL, "
      + MetaDataColumns.URI + " TEXT NOT NULL DEFAULT '', "
      + MetaDataColumns.TIME + " TEXT, " + MetaDataColumns.SEQUENCE
      + " INTEGER NOT NULL DEFAULT '-1', " + MetaDataColumns.DIRTY
      + " INTEGER NOT NULL DEFAULT '0', " + MetaDataColumns.LOCKED
      + " INTEGER NOT NULL DEFAULT '0', " + MetaDataColumns.CONFLICT
      + " INTEGER NOT NULL DEFAULT '0', " + "PRIMARY KEY(" + MetaDataColumns.ID
      + "))";

  private static final String CREATE_TABLE_LOCAL_CONFLICTS = "CREATE TABLE "
      + Tables.LOCAL_CONFLICTS + "(" + LocalConflictColumns.ID
      + " INTEGER NOT NULL, " + LocalConflictColumns.FILE_ID
      + " TEXT NOT NULL UNIQUE, " + LocalConflictColumns.NEWFILE_URI
      + " TEXT NOT NULL, " + LocalConflictColumns.NEWFILE_TIMESTAMP
      + " TEXT NOT NULL," + "PRIMARY KEY(" + LocalConflictColumns.ID
      + "), FOREIGN KEY(" + LocalConflictColumns.FILE_ID + ") REFERENCES "
      + Tables.METADATA + "(" + MetaDataColumns.FILE_ID
      + ") ON DELETE CASCADE)";

  private static final String CREATE_TABLE_BACKEND_CONFLICTS = "CREATE TABLE "
      + Tables.BACKEND_CONFLICTS + "(" + BackendConflictColumns.ID
      + " INTEGER NOT NULL, " + BackendConflictColumns.FILE_ID
      + " TEXT NOT NULL, " + BackendConflictColumns.BACKEND_SEQUENCE
      + " INTEGER NOT NULL, " + BackendConflictColumns.BACKEND_TIMESTAMP
      + " TEXT NOT NULL, " + BackendConflictColumns.RESOLVED + " TEXT DEFAULT "
      + BackendResolve.UNRESOLVED.name() + ", " + " PRIMARY KEY("
      + BackendConflictColumns.ID + "), FOREIGN KEY("
      + BackendConflictColumns.FILE_ID + ") REFERENCES " + Tables.METADATA
      + "(" + MetaDataColumns.FILE_ID + ") ON DELETE CASCADE)";

  // ---------------------------------------------------------------------------
  // ---------------------------------------------------------------------------

  public GenericDb(Context context)
  {
    super(context, DB_NAME, null, DB_VERSION);
    Utils.printMethodName(TAG);
  }// ctor

  // ---------------------------------------------------------------------------

  @Override
  public void onCreate(SQLiteDatabase db)
  {
    Utils.printMethodName(TAG);
    db.execSQL(CREATE_TABLE_METADATA);
    db.execSQL(CREATE_TABLE_LOCAL_CONFLICTS);
    db.execSQL(CREATE_TABLE_BACKEND_CONFLICTS);
  }// onCreate

  // ---------------------------------------------------------------------------

  @Override
  public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion)
  {
    Utils.printMethodName(TAG);
    if(oldVersion == 1 && newVersion == 2)
    {
      db.execSQL("DROP TABLE IF EXISTS " + Tables.BACKEND_CONFLICTS);
      db.execSQL(CREATE_TABLE_BACKEND_CONFLICTS);
    }
  }// onUpgrade

  // ---------------------------------------------------------------------------

  public Cursor queryFile(String fileId, String[] projectionIn)
  {
    Utils.printMethodName(TAG);
    SQLiteQueryBuilder queryBuilder = new SQLiteQueryBuilder();

    queryBuilder.setTables(Tables.METADATA);

    String[] selectionArg = { fileId };
    Cursor result = queryBuilder.query(getReadableDatabase(), projectionIn,
        MetaDataColumns.FILE_ID + "=?", selectionArg, null, null, null);
    if( !result.moveToFirst() )
    {
      return null;
    }
    return result;
  }// queryFile

  // ---------------------------------------------------------------------------

  public Uri getFileUri(String fileId)
  {
    Utils.printMethodName(TAG);
    Uri result = null;
    Cursor c = null;
    String[] projectionIn = { MetaDataColumns.URI };
    if( (c = queryFile(fileId, projectionIn)) != null )
    {
      String fileUriStr = c.getString(0);
      result = Uri.parse(fileUriStr == null ? "" : fileUriStr);
      c.close();
    }
    return result;
  }// getFileUri

  // ---------------------------------------------------------------------------

  public MetaData getFileMetaDataFromConflictId(long conflictId,
      String conflictTable)
  {
    Utils.printMethodName(TAG);
    SQLiteQueryBuilder queryBuilder = new SQLiteQueryBuilder();

    queryBuilder.setTables(conflictTable + " JOIN " + Tables.METADATA + " ON "
        + conflictTable + "." + MetaDataColumns.FILE_ID + "=" + Tables.METADATA
        + "." + MetaDataColumns.FILE_ID);
    queryBuilder.appendWhere(conflictTable + "." + MetaDataColumns.ID + "="
        + conflictId);
    Cursor c = queryBuilder.query(getWritableDatabase(),
        MetaDataColumns.METADATA_PROJ, null, null, null, null, null);

    MetaData result = null;
    if( c.moveToFirst() )
    {
      result = new MetaData(c);
    }
    return result;
  }

  // ---------------------------------------------------------------------------

  public MetaData getFileMetaData(String fileId)
  {
    Utils.printMethodName(TAG);
    MetaData result = null;
    Cursor c = null;
    if( (c = queryFile(fileId, MetaDataColumns.METADATA_PROJ)) != null )
    {
      result = new MetaData(c);
      c.close();
    }
    return result;
  }// getFileMetaData

  // ---------------------------------------------------------------------------

  public int updateFile(MetaData file, ContentValues values)
  {
    Utils.printMethodName(TAG);
    String[] whereArgs = { file.fileId(),
        Integer.toString(file.sequenceNumber()) };
    return getWritableDatabase().update(Tables.METADATA, values,
        MetaDataColumns.FILE_ID + "=? AND " + MetaDataColumns.SEQUENCE + "=?",
        whereArgs);
  }// updateFile

  // ---------------------------------------------------------------------------

  public long newFile(ContentValues values)
  {
    Utils.printMethodName(TAG);
    return getWritableDatabase().insert(Tables.METADATA, null, values);
  }// newFile

  // ---------------------------------------------------------------------------

  public void newLocalConflict(MetaData file, ContentValues intendedUpdateValues)
  {
    Utils.printMethodName(TAG);
    ContentValues values = new ContentValues();
    values.put(LocalConflictColumns.FILE_ID, file.fileId());
    values.put(LocalConflictColumns.NEWFILE_URI,
        intendedUpdateValues.getAsString(MetaDataColumns.URI));
    values.put(LocalConflictColumns.NEWFILE_TIMESTAMP,
        intendedUpdateValues.getAsString(MetaDataColumns.TIME));
    getWritableDatabase().insert(Tables.LOCAL_CONFLICTS, null, values);
    values.clear();
    values.put(MetaDataColumns.CONFLICT, true);
    String[] whereArgs = { file.fileId() };
    getWritableDatabase().update(Tables.METADATA, values,
        MetaDataColumns.FILE_ID + "=?", whereArgs);
  }// newLocalConflict

  public List<Uri> getAllFilesFor(MetaData m)
  {
    ArrayList<Uri> result = new ArrayList<Uri>();
    result.add(m.fileUri());

    SQLiteQueryBuilder queryBuilder = new SQLiteQueryBuilder();
    queryBuilder.setTables(Tables.LOCAL_CONFLICTS);

    String[] projectionIn = { LocalConflictColumns.NEWFILE_URI };
    String[] selectionArgs = { m.fileId() };
    Cursor c = queryBuilder.query(getWritableDatabase(), projectionIn,
        LocalConflictColumns.FILE_ID + "=?", selectionArgs, null, null, null);
    if( c.moveToFirst() )
    {
      do
      {
        result.add(Uri.parse(c.getString(0)));
      } while( c.moveToNext() );
    }
    return result;
  }

  public long newBackendConflict(ContentValues values)
  {
    Utils.printMethodName(TAG);
    SQLiteDatabase db = getWritableDatabase();

    // upsert to backend conflict table
    long conflictId = db.insertWithOnConflict(Tables.BACKEND_CONFLICTS, null,
        values, SQLiteDatabase.CONFLICT_IGNORE);

    updateBackendConflict(conflictId, values);

    return conflictId;
  }

  // ---------------------------------------------------------------------------

  public int updateBackendConflict(long conflictId, ContentValues values)
  {
    Utils.printMethodName(TAG);
    SQLiteDatabase db = getWritableDatabase();
    String[] whereArgs = { values.getAsString(BackendConflictColumns.FILE_ID) };
    db.update(Tables.BACKEND_CONFLICTS, values, BackendConflictColumns.FILE_ID
        + "=?", whereArgs);

    // flag file metadata as conflicted
    ContentValues metadataVal = new ContentValues();
    metadataVal.put(MetaDataColumns.CONFLICT, true);
    return db.update(Tables.METADATA, metadataVal, MetaDataColumns.FILE_ID
        + "=?", whereArgs);
  }

  // ---------------------------------------------------------------------------

  public void dumpDB()
  {
    Utils.printMethodName(TAG);
    Cursor c = getReadableDatabase().query(Tables.METADATA, null, null, null,
        null, null, null);
    DatabaseUtils.dumpCursor(c);
    c.close();
    c = getReadableDatabase().query(Tables.LOCAL_CONFLICTS, null, null, null,
        null, null, null);
    DatabaseUtils.dumpCursor(c);
    c.close();
  }// dumpDB

  // ---------------------------------------------------------------------------

}
