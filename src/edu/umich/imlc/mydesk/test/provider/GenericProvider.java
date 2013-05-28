package edu.umich.imlc.mydesk.test.provider;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream.GetField;
import java.nio.channels.FileChannel;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import edu.umich.imlc.mydesk.test.common.GenericContract;
import edu.umich.imlc.mydesk.test.common.GenericContract.GenericURIs;
import edu.umich.imlc.mydesk.test.common.GenericContract.MetaDataColumns;
import edu.umich.imlc.mydesk.test.common.GenericContract.Tables;
import edu.umich.imlc.mydesk.test.common.Utils;
import edu.umich.imlc.mydesk.test.db.GenericDb;
import edu.umich.imlc.mydesk.test.service.GenericSyncService;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.content.SharedPreferences;
import android.content.UriMatcher;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteQueryBuilder;
import android.net.Uri;
import android.os.ParcelFileDescriptor;
import android.util.Log;
import edu.umich.imlc.mydesk.test.common.GenericContract.*;

public class GenericProvider extends ContentProvider
{
  public static final String TAG = "GenericProvider";

  private static final int FILES = 100;
  private static final int FILE = 101;
  private static final int CURRENT_ACCOUNT = 110;
  private static final int BACKEND_CONFLICTS = 120;
  private static final int BACKEND_CONFLICT = 121;
  private static final int LOCAL_CONFLICTS = 130;
  private static final int LOCAL_CONFLICT = 131;
  private static final UriMatcher sURIMatcher = new UriMatcher(
      UriMatcher.NO_MATCH);
  static
  {
    sURIMatcher.addURI(GenericContract.AUTHORITY, "files", FILES);
    sURIMatcher.addURI(GenericContract.AUTHORITY, "files/*", FILE);
    sURIMatcher.addURI(GenericContract.AUTHORITY, "current_account",
        CURRENT_ACCOUNT);
    sURIMatcher.addURI(GenericContract.AUTHORITY, "backend_conflicts",
        BACKEND_CONFLICTS);
    sURIMatcher.addURI(GenericContract.AUTHORITY, "backend_conflicts/*",
        BACKEND_CONFLICT);
    sURIMatcher.addURI(GenericContract.AUTHORITY, "local_conflicts",
        LOCAL_CONFLICTS);
    sURIMatcher.addURI(GenericContract.AUTHORITY, "local_conflicts/*",
        LOCAL_CONFLICT);
  }
  private GenericDb genericDb;
  private SharedPreferences prefs;

  // ---------------------------------------------------------------------------
  // ---------------------------------------------------------------------------

  public GenericProvider()
  {
    Utils.printMethodName(TAG);
  }

  // ---------------------------------------------------------------------------

  @Override
  public int delete(Uri uri, String selection, String[] selectionArgs)
  {
    Utils.printMethodName(TAG);
    switch ( sURIMatcher.match(uri) )
    {
      case FILE:
        MetaData m = genericDb.getFileMetaData(uri.getLastPathSegment());
        List<Uri> uris = genericDb.getAllFilesFor(m);

        // delete entry for the metadata table, this will cascade to the
        // conflict tables
        String[] whereArgs = { m.fileId() };
        genericDb.getWritableDatabase().delete(Tables.METADATA,
            MetaDataColumns.FILE_ID + "=?", whereArgs);
        for( Uri u : uris )
        {
          File f = new File(u.getPath());
          f.delete();
        }
        getContext().getContentResolver().notifyChange(uri, null,
            !testQueryParam(GenericContract.CALLER_IS_SYNC_ADAPTER, uri));
        return 1;
      default:
        throw new IllegalArgumentException("Invalid URI: " + uri.toString());
    }
  }

  // ---------------------------------------------------------------------------

  @Override
  public String getType(Uri uri)
  {
    Utils.printMethodName(TAG);
    switch ( sURIMatcher.match(uri) )
    {
      case FILE:
        SQLiteQueryBuilder queryBuilder = new SQLiteQueryBuilder();
        queryBuilder.appendWhereEscapeString(MetaDataColumns.FILE_ID + "='"
            + uri.getLastPathSegment() + "'");
        String[] projectionIn = { MetaDataColumns.TYPE };
        Cursor c = queryBuilder.query(genericDb.getReadableDatabase(),
            projectionIn, null, null, null, null, null);
        if( c.moveToFirst() )
        {
          return c.getString(0);
        }
      case FILES:
      default:
        return null;
    }
  }

  // ---------------------------------------------------------------------------

  @Override
  public Uri insert(Uri uri, ContentValues values)
  {
    Utils.printMethodName(TAG);
    genericDb.getWritableDatabase().beginTransaction();
    try
    {
      switch ( sURIMatcher.match(uri) )
      {
        case FILES:
          Uri newFile;
          if( !testQueryParam(GenericContract.CALLER_IS_SYNC_ADAPTER, uri) )
          {
            newFile = newLocalFile(uri, values);
          }
          else
          {
            newFile = newBackendFile(values);
          }
          genericDb.getWritableDatabase().setTransactionSuccessful();
          getContext().getContentResolver().notifyChange(GenericURIs.URI_FILES,
              null,
              !testQueryParam(GenericContract.CALLER_IS_SYNC_ADAPTER, uri));
          return newFile;
        case BACKEND_CONFLICT:
          if( !testQueryParam(GenericContract.CALLER_IS_SYNC_ADAPTER, uri) )
          {
            throw new IllegalArgumentException("Caller not syncAdapter");
          }
          Uri newBackendConflict = Uri.withAppendedPath(
              GenericURIs.URI_BACKEND_CONFLICTS,
              Long.toString(genericDb.newBackendConflict(values)));
          genericDb.getWritableDatabase().setTransactionSuccessful();
          return newBackendConflict;
        default:
          throw new IllegalArgumentException("Invalid URI: " + uri.toString());
      }
    }
    catch( IOException e )
    {
      e.printStackTrace();
      throw new IllegalStateException(e);
    }
    finally
    {
      genericDb.getWritableDatabase().endTransaction();
    }
  }

  // ---------------------------------------------------------------------------

  @Override
  public boolean onCreate()
  {
    Utils.printMethodName(TAG);
    genericDb = new GenericDb(getContext());
    return true;
  }

  // ---------------------------------------------------------------------------

  @Override
  public Cursor query(Uri uri, String[] projection, String selection,
      String[] selectionArgs, String sortOrder)
  {
    Utils.printMethodName(TAG);
    Cursor c;
    switch ( sURIMatcher.match(uri) )
    {
      case FILE:
        c = genericDb.queryFile(uri.getLastPathSegment(), projection);
        break;
      case FILES:
        SQLiteQueryBuilder queryBuilder = new SQLiteQueryBuilder();
        queryBuilder.setTables(Tables.METADATA);
        String owner = null;
        owner = uri.getQueryParameter(MetaDataColumns.OWNER);
        if( owner == null )
        {
          owner = getUser();
        }
        queryBuilder.appendWhere(MetaDataColumns.OWNER + "='" + owner + "'");
        c = queryBuilder.query(genericDb.getReadableDatabase(), projection,
            selection, selectionArgs, null, null, sortOrder);
        break;
      case CURRENT_ACCOUNT:
        String[] colNames = { GenericContract.PREFS_ACCOUNT_NAME };
        MatrixCursor mc = new MatrixCursor(colNames);
        mc.newRow().add(getUser());
        c = mc;
        break;
      case BACKEND_CONFLICTS:
        c = getConflicts(uri, projection, selection, selectionArgs, sortOrder,
            Tables.BACKEND_CONFLICTS);
        break;
      case LOCAL_CONFLICTS:
        c = getConflicts(uri, projection, selection, selectionArgs, sortOrder,
            Tables.LOCAL_CONFLICTS);
        break;
      default:
        throw new IllegalArgumentException("Unknown URI: " + uri.toString());
    }
    c.setNotificationUri(getContext().getContentResolver(), uri);
    return c;
  }

  /**
   * Returns a cursor containing entries in the specified conflicts table, owned
   * by the specified user if the uri is appended with the query string "owner",
   * otherwise the currently logged in account is used
   * 
   * @param uri
   * @param projection
   * @param selection
   * @param selectionArgs
   * @param sortOrder
   * @return
   */
  private Cursor getConflicts(Uri uri, String[] projection, String selection,
      String[] selectionArgs, String sortOrder, String conflictTable)
  {
    String owner = null;
    owner = uri.getQueryParameter(MetaDataColumns.OWNER);
    if( owner == null )
    {
      owner = getUser();
    }
    SQLiteQueryBuilder queryBuilder = new SQLiteQueryBuilder();
    queryBuilder.setTables(conflictTable + " AS " + conflictTable + " JOIN "
        + Tables.METADATA + " AS " + Tables.METADATA + " ON " + conflictTable
        + "." + MetaDataColumns.FILE_ID + "=" + Tables.METADATA + "."
        + MetaDataColumns.FILE_ID);
    queryBuilder.appendWhere(MetaDataColumns.OWNER + "='" + owner + "'");
    return queryBuilder.query(genericDb.getReadableDatabase(), projection,
        selection, selectionArgs, null, null, sortOrder);
  }

  // ---------------------------------------------------------------------------

  @Override
  public int update(Uri uri, ContentValues values, String selection,
      String[] selectionArgs)
  {
    Utils.printMethodName(TAG);
    switch ( sURIMatcher.match(uri) )
    {
      case FILE:
        if( testQueryParam(GenericContract.UPDATE_METADATA, uri) )
        {
          // update doesn't touch the actual file
          return updateMetaDataOnly(
              genericDb.getFileMetaData(uri.getLastPathSegment()), values);
        }
        genericDb.getWritableDatabase().beginTransaction();
        try
        {
          // remember the old file and check if it exists
          MetaData fileMetaData;
          if( (fileMetaData = genericDb.getFileMetaData(uri
              .getLastPathSegment())) == null )
          {
            throw new IllegalArgumentException("file id doesn't exist");
          }
          boolean conflict = save(uri, values, fileMetaData);
          genericDb.getWritableDatabase().setTransactionSuccessful();
          getContext().getContentResolver().notifyChange(uri, null,
              !testQueryParam(GenericContract.CALLER_IS_SYNC_ADAPTER, uri));
          if( conflict )
          {
            GenericSyncService.displayConflictNotification(getContext()
                .getApplicationContext());
            return 0;
          }
          return 1;
        }
        finally
        {
          genericDb.getWritableDatabase().endTransaction();
        }
      case FILES:
      {
        if( testQueryParam(GenericContract.UNLOCK_FILES, uri) )
        {
          return setLocks(uri.getQueryParameter(MetaDataColumns.OWNER), false);
        }
        if( testQueryParam(GenericContract.LOCK_FILES, uri) )
        {
          return setLocks(uri.getQueryParameter(MetaDataColumns.OWNER), true);
        }
      }
      case BACKEND_CONFLICT:
      {
        genericDb.getWritableDatabase().beginTransaction();
        long conflictId = Long.valueOf(uri.getLastPathSegment());
        try
        {
          MetaData m = null;
          if( (m = genericDb.getFileMetaDataFromConflictId(conflictId,
              Tables.BACKEND_CONFLICTS)) != null )
          {
            if( !testQueryParam(GenericContract.UPDATE_METADATA, uri) )
            {
              // either keep backend successful, or it's an update to the
              // conflict table
              if( values.containsKey(BackendConflictColumns.BACKEND_SEQUENCE) )
              {
                // update conflict table
                int result = genericDb
                    .updateBackendConflict(conflictId, values);
                if( result == 1 )
                {
                  genericDb.getWritableDatabase().setTransactionSuccessful();
                }
                getContext().getContentResolver()
                    .notifyChange(
                        uri,
                        null,
                        !testQueryParam(GenericContract.CALLER_IS_SYNC_ADAPTER,
                            uri));
                return result;
              }
              // keep backend success
              if( save(uri, values, m) )
              {
                GenericSyncService.displayConflictNotification(getContext()
                    .getApplicationContext());
              }
            }
            else
            {
              // keep local success
              updateMetaDataOnly(m, values);
            }
          }
          // conflict resolution successful, delete entry
          int result = genericDb.getWritableDatabase().delete(
              Tables.BACKEND_CONFLICTS,
              BackendConflictColumns.ID + "=" + conflictId, null);

          getContext().getContentResolver().notifyChange(uri, null,
              !testQueryParam(GenericContract.CALLER_IS_SYNC_ADAPTER, uri));
          getContext().getContentResolver().notifyChange(GenericURIs.URI_FILES,
              null,
              !testQueryParam(GenericContract.CALLER_IS_SYNC_ADAPTER, uri));
          return result;
        }
        finally
        {
          genericDb.getWritableDatabase().endTransaction();
        }
      }
      case LOCAL_CONFLICT:
      default:
        throw new UnsupportedOperationException("Unknown URI: "
            + uri.toString());
    }
  }// update

  // ---------------------------------------------------------------------------

  private int updateMetaDataOnly(MetaData m, ContentValues values)
  {
    values.remove(MetaDataColumns.URI);
    return genericDb.updateFile(m, values);
  }

  // ---------------------------------------------------------------------------

  // toggles the locked flag of all files owned by owner
  private int setLocks(String owner, boolean locked)
  {
    Utils.printMethodName(TAG);
    ContentValues v = new ContentValues();
    v.put(MetaDataColumns.LOCKED, locked);
    String[] whereArgs = { owner };
    return genericDb.getWritableDatabase().update(Tables.METADATA, v,
        MetaDataColumns.OWNER + "=?", whereArgs);
  }// setLocks

  // ---------------------------------------------------------------------------

  private boolean save(Uri uri, ContentValues values, MetaData fileMetaData)
  {
    Utils.printMethodName(TAG);
    try
    {
      boolean fromBackend = testQueryParam(
          GenericContract.CALLER_IS_SYNC_ADAPTER, uri);
      if( fileMetaData.locked() && !fromBackend )
      {
        // file is locked and someone other than the sync adapter is trying to
        // access it
        throw new IllegalStateException(Exceptions.FILELOCKEDEXCEPTION.name());
      }
      Uri oldFile = fileMetaData.fileUri();

      File fromFile = new File(Uri.parse(
          values.getAsString(GenericContract.KEY_NEW_FILE)).getPath());
      File toFile = null;
      long newSeq;
      String newTime;
      if( fromBackend )
      {
        // new file already in internal storage
        toFile = fromFile;

        // populate backend specific column values
        newSeq = values.getAsLong(MetaDataColumns.SEQUENCE);
        newTime = values.getAsString(MetaDataColumns.TIME);
      }
      else
      {
        // check for ownership
        String currentUser = getUser();
        if( currentUser.isEmpty() )
        {
          throw new IllegalStateException(Exceptions.NOUSEREXCEPTION.name());
        }
        if( !currentUser.equals(fileMetaData.Owner()) )
        {
          throw new IllegalStateException(Exceptions.NOTOWNEREXCEPTION.name());
        }

        // copy new file to internal storage
        toFile = new File(Utils.createRandomFileUri(getContext().getFilesDir())
            .getPath());

        copyFile(fromFile, toFile);

        // populate local specific column values
        newTime = GenericContract.INTERNAL_DATE_FORMAT.format(new Date());
        newSeq = fileMetaData.sequenceNumber();
        if( !fileMetaData.dirty() )
        {
          newSeq++;
        }
      }

      // prepare update values
      ContentValues updateValues = prepareMetaDataUpdate(Uri.fromFile(toFile),
          newSeq, newTime, fromBackend);

      // attempt to save
      if( genericDb.updateFile(fileMetaData, updateValues) != 1 )
      {
        // conflict
        genericDb.newLocalConflict(fileMetaData, updateValues);
      }
      // delete the oldFile if it exists
      if( !oldFile.toString().isEmpty() )
      {
        new File(oldFile.getPath()).delete();
      }
      return false;
    }
    catch( IOException e )
    {
      e.printStackTrace();
      throw new IllegalStateException(e);
    }
  }// save

  // ---------------------------------------------------------------------------

  ContentValues prepareMetaDataUpdate(Uri newUri, long newSequence,
      String newTimestamp, boolean fromBackend)
  {
    Utils.printMethodName(TAG);
    ContentValues v = new ContentValues();
    v.put(MetaDataColumns.DIRTY, !fromBackend);
    v.put(MetaDataColumns.URI, newUri.toString());
    v.put(MetaDataColumns.SEQUENCE, newSequence);
    v.put(MetaDataColumns.TIME, newTimestamp);
    v.put(MetaDataColumns.LOCKED, false);
    v.put(MetaDataColumns.CONFLICT, false);
    return v;
  }

  /*
   * (non-Javadoc)
   * 
   * @see android.content.ContentProvider#openFile(android.net.Uri,
   * java.lang.String)
   */
  @Override
  public ParcelFileDescriptor openFile(Uri uri, String mode)
      throws FileNotFoundException
  {
    Utils.printMethodName(TAG);
    switch ( sURIMatcher.match(uri) )
    {
      case FILE:
        break;
      case FILES:
      default:
        throw new IllegalArgumentException("Unknown URI: " + uri.toString());
    }

    MetaData file = genericDb.getFileMetaData(uri.getLastPathSegment());
    if( file == null )
    {
      throw new FileNotFoundException("File Id doesn't exist");
    }
    if( file.locked()
        && !testQueryParam(GenericContract.CALLER_IS_SYNC_ADAPTER, uri) )
    {
      // file is locked and someone other than the sync adapter is trying to
      // access it
      throw new IllegalStateException(Exceptions.FILELOCKEDEXCEPTION.name());
    }

    File privateFile = new File(file.fileUri().getPath());
    if( !privateFile.exists() )
    {
      throw new FileNotFoundException("File doesn't exist on disk");
    }
    return ParcelFileDescriptor.open(privateFile,
        ParcelFileDescriptor.MODE_READ_ONLY);
  }// openFile

  // ---------------------------------------------------------------------------

  private String getRandomFileId()
  {
    Utils.printMethodName(TAG);
    return UUID.randomUUID().toString() + "__" + UUID.randomUUID().toString();
  }// getRandomFileId

  // ---------------------------------------------------------------------------

  private void copyFile(File from, File to) throws IOException
  {
    Utils.printMethodName(TAG);
    FileInputStream is = new FileInputStream(from);
    FileOutputStream os = new FileOutputStream(to);
    try
    {
      FileChannel fromChannel = is.getChannel();
      FileChannel toChannel = os.getChannel();

      toChannel.transferFrom(fromChannel, 0, fromChannel.size());
    }
    finally
    {
      is.close();
      os.close();
    }
  }// copyFile

  // ---------------------------------------------------------------------------

  private Uri newLocalFile(Uri uri, ContentValues values) throws IOException
  {
    Utils.printMethodName(TAG);
    String owner = getUser();
    if( owner.isEmpty() )
    {
      throw new IllegalStateException(Exceptions.NOUSEREXCEPTION.name());
    }
    ContentValues newFileValues = new ContentValues();
    String newId = getRandomFileId();
    newFileValues.put(MetaDataColumns.FILE_ID, newId);
    newFileValues.put(MetaDataColumns.OWNER, owner);
    newFileValues.put(MetaDataColumns.NAME,
        values.getAsString(MetaDataColumns.NAME));
    newFileValues.put(MetaDataColumns.TYPE,
        values.getAsString(MetaDataColumns.TYPE));
    if( genericDb.newFile(newFileValues) == -1 )
    {
      throw new IllegalStateException("SQL error");
    }

    MetaData file = genericDb.getFileMetaData(newId);
    // copy new file to internal storage
    File fromFile = new File(Uri.parse(
        values.getAsString(GenericContract.KEY_NEW_FILE)).getPath());
    File toFile = new File(Utils
        .createRandomFileUri(getContext().getFilesDir()).getPath());

    copyFile(fromFile, toFile);

    ContentValues updateValues = prepareMetaDataUpdate(Uri.fromFile(toFile), 0,
        GenericContract.INTERNAL_DATE_FORMAT.format(new Date()), false);

    // update the metadata information
    genericDb.updateFile(file, updateValues);

    return Uri.withAppendedPath(GenericURIs.URI_FILES, newId);
  }// newFile

  // ---------------------------------------------------------------------------

  private Uri newBackendFile(ContentValues values)
  {
    Utils.printMethodName(TAG);
    genericDb.newFile(values);
    MetaData file = genericDb.getFileMetaData(values
        .getAsString(MetaDataColumns.FILE_ID));
    return Uri.withAppendedPath(GenericURIs.URI_FILES, file.fileId());
  }// newBackendFile

  // ---------------------------------------------------------------------------

  private String getUser()
  {
    Utils.printMethodName(TAG);
    if( prefs == null )
    {
      prefs = getContext().getSharedPreferences(GenericContract.SHARED_PREFS,
          Context.MODE_PRIVATE);
    }
    String s = prefs.getString(GenericContract.PREFS_ACCOUNT_NAME, "");
    Log.i(TAG, String.format("User: %s", s));
    return s;
  }// getUser

  // ---------------------------------------------------------------------------

  /**
   * Determines if the given URI specifies that the request is coming from the
   * sync adapter.
   * 
   * @param uri
   *          the given URI
   * @return true if the uri specifies that the request is coming from the sync
   *         adapter
   */
  private boolean testQueryParam(String param, Uri uri)
  {
    Utils.printMethodName(TAG);
    final String queryParam = uri.getQueryParameter(param);
    return queryParam != null && queryParam.equals("true");
  }

}// GenericProvider
