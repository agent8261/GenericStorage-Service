package edu.umich.imlc.mydesk.test.db;

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
      + " TEXT NOT NULL," + LocalConflictColumns.RESOLVED
      + " TEXT DEFAULT NULL, " + "PRIMARY KEY(" + LocalConflictColumns.ID
      + "), FOREIGN KEY(" + LocalConflictColumns.FILE_ID + ") REFERENCES "
      + Tables.METADATA + "(" + MetaDataColumns.FILE_ID
      + ") ON DELETE CASCADE)";

  private static final String CREATE_TABLE_BACKEND_CONFLICTS = "CREATE TABLE "
      + Tables.BACKEND_CONFLICTS + "(" + BackendConflictColumns.ID
      + " INTEGER NOT NULL, " + BackendConflictColumns.FILE_ID
      + " TEXT NOT NULL, " + BackendConflictColumns.RESOLVED
      + " TEXT DEFAULT NULL, " + " PRIMARY KEY(" + BackendConflictColumns.ID
      + "), FOREIGN KEY(" + BackendConflictColumns.FILE_ID + ") REFERENCES "
      + Tables.METADATA + "(" + MetaDataColumns.FILE_ID
      + ") ON DELETE CASCADE)";

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
    db.execSQL("DROP TABLE IF EXISTS " + Tables.METADATA);
    db.execSQL("DROP TABLE IF EXISTS " + Tables.LOCAL_CONFLICTS);
    onCreate(db);
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

  public MetaData getFileMetaData(String fileId)
  {
    Utils.printMethodName(TAG);
    MetaData result = null;
    Cursor c = null;
    if( (c = queryFile(fileId, MetaDataProjections.METADATA)) != null )
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


  public long newBackendConflict(ContentValues values)
  {
    Utils.printMethodName(TAG);
    SQLiteDatabase db = getWritableDatabase();

    // upsert to backend conflict table
    long conflictId = db.insertWithOnConflict(Tables.BACKEND_CONFLICTS, null,
        values, SQLiteDatabase.CONFLICT_IGNORE);
    String[] whereArgs = { values.getAsString(BackendConflictColumns.FILE_ID) };
    db.update(Tables.BACKEND_CONFLICTS, values, BackendConflictColumns.FILE_ID
        + "=?", whereArgs);

    // flag file metadata as conflicted
    ContentValues metadataVal = new ContentValues();
    metadataVal.put(MetaDataColumns.CONFLICT, true);
    db.update(Tables.METADATA, metadataVal, MetaDataColumns.FILE_ID + "=?",
        whereArgs);

    return conflictId;
  }

  public int deleteAllUserMetaData(String user)
  {

    return 0;
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
