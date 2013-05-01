package edu.umich.imlc.mydesk.test.provider;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.text.DateFormat;
import java.util.Date;
import java.util.UUID;

import edu.umich.imlc.mydesk.test.common.GenericContract;
import edu.umich.imlc.mydesk.test.common.Utils;
import edu.umich.imlc.mydesk.test.db.GenericDb;
import edu.umich.imlc.mydesk.test.service.ConflictActivity;

import android.app.NotificationManager;
import android.app.PendingIntent;
import android.app.TaskStackBuilder;
import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.content.Intent;
import android.content.SharedPreferences;
import android.content.UriMatcher;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteQueryBuilder;
import android.net.Uri;
import android.os.ParcelFileDescriptor;
import android.support.v4.app.NotificationCompat;
import android.util.Log;
import edu.umich.imlc.mydesk.test.common.GenericContract.*;

public class GenericProvider extends ContentProvider
{
  public static final String TAG = "GenericProvider";

  private static final int FILES = 100;
  private static final int FILE = 110;
  private static final int CURRENT_ACCOUNT = 120;
  private static final UriMatcher sURIMatcher = new UriMatcher(
      UriMatcher.NO_MATCH);
  private static final int NOTIFICATION_CONFLICT = 1010;
  static
  {
    sURIMatcher.addURI(GenericContract.AUTHORITY, "files", FILES);
    sURIMatcher.addURI(GenericContract.AUTHORITY, "files/*", FILE);
    sURIMatcher.addURI(GenericContract.AUTHORITY, "current_account",
        CURRENT_ACCOUNT);
  }
  private GenericDb genericDb;
  private SharedPreferences prefs;

  // ---------------------------------------------------------------------------
  // ---------------------------------------------------------------------------

  public GenericProvider()
  {
    Utils.printMethodName();
  }

  // ---------------------------------------------------------------------------

  @Override
  public int delete(Uri arg0, String arg1, String[] arg2)
  {
    Utils.printMethodName();
    throw new UnsupportedOperationException("delete not supported");
  }

  // ---------------------------------------------------------------------------

  @Override
  public String getType(Uri uri)
  {
    Utils.printMethodName();
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
    Utils.printMethodName();
    switch ( sURIMatcher.match(uri) )
    {
      case FILES:
        genericDb.getWritableDatabase().beginTransaction();
        try
        {
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
          getContext().getContentResolver().notifyChange(
              GenericContract.URI_FILES, null,
              !testQueryParam(GenericContract.CALLER_IS_SYNC_ADAPTER, uri));
          return newFile;
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
      default:
        throw new IllegalArgumentException("Invalid URI: " + uri.toString());
    }
  }

  // ---------------------------------------------------------------------------

  @Override
  public boolean onCreate()
  {
    Utils.printMethodName();
    genericDb = new GenericDb(getContext());
    return true;
  }

  // ---------------------------------------------------------------------------

  @Override
  public Cursor query(Uri uri, String[] projection, String selection,
      String[] selectionArgs, String sortOrder)
  {
    Utils.printMethodName();
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
      default:
        throw new IllegalArgumentException("Unknown URI: " + uri.toString());
    }

    c.setNotificationUri(getContext().getContentResolver(), uri);
    return c;
  }

  // ---------------------------------------------------------------------------

  @Override
  public int update(Uri uri, ContentValues values, String selection,
      String[] selectionArgs)
  {
    Utils.printMethodName();
    switch ( sURIMatcher.match(uri) )
    {
      case FILE:
        genericDb.getWritableDatabase().beginTransaction();
        try
        {
          if( testQueryParam(GenericContract.LOCK_FILE, uri) )
          {
            MetaData m = genericDb.getFileMetaData(uri.getLastPathSegment());
            genericDb.updateFile(m, values);
            genericDb.getWritableDatabase().setTransactionSuccessful();
          }
          else
          {
            boolean conflict = save(uri, values);
            genericDb.getWritableDatabase().setTransactionSuccessful();
            getContext().getContentResolver().notifyChange(uri, null,
                !testQueryParam(GenericContract.CALLER_IS_SYNC_ADAPTER, uri));
            if( conflict )
            {
              displayConflictNotification();
              return 0;
            }
          }
          return 1;
        }
        finally
        {
          genericDb.getWritableDatabase().endTransaction();
        }
      case FILES:
      {
        if( testQueryParam(GenericContract.CLEAN_FILE, uri) )
        {
          ContentValues v = new ContentValues();
          v.put(MetaDataColumns.LOCKED, false);
          return genericDb.getWritableDatabase()
              .update(Tables.METADATA, v, null, null);
        }
      }
      default:
        throw new UnsupportedOperationException("Unknown URI: "
            + uri.toString());
    }
  }// update

  // ---------------------------------------------------------------------------

  private boolean save(Uri uri, ContentValues values)
  {
    Utils.printMethodName();
    try
    {
      // remember the old file and check if it exists
      MetaData fileMetaData;
      if( (fileMetaData = genericDb.getFileMetaData(uri.getLastPathSegment())) == null )
      {
        throw new IllegalArgumentException("file id doesn't exist");
      }
      Uri oldFile = fileMetaData.fileUri();

      File fromFile = new File(Uri.parse(
          values.getAsString(GenericContract.KEY_NEW_FILE)).getPath());
      File toFile = null;
      long newSeq;
      String newTime;
      boolean fromBackend = values
          .getAsBoolean(GenericContract.KEY_UPDATE_BACKEND);
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
        newTime = DateFormat.getDateTimeInstance().format(new Date());
        newSeq = fileMetaData.sequenceNumber();
        if( !fileMetaData.dirty() )
        {
          newSeq++;
        }
      }

      // prepare update values
      ContentValues updateValues = prepareMetaDataUpdate(Uri.fromFile(toFile),
          newSeq, newTime);

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
      String newTimestamp)
  {
    Utils.printMethodName();
    ContentValues v = new ContentValues();
    v.put(MetaDataColumns.DIRTY, true);
    v.put(MetaDataColumns.URI, newUri.toString());
    v.put(MetaDataColumns.SEQUENCE, newSequence);
    v.put(MetaDataColumns.TIME, newTimestamp);
    v.put(MetaDataColumns.LOCKED, false);
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
    Utils.printMethodName();
    switch ( sURIMatcher.match(uri) )
    {
      case FILE:
        break;
      case FILES:
      default:
        throw new IllegalArgumentException("Unknown URI: " + uri.toString());
    }

    Uri fileUri = genericDb.getFileUri(uri.getLastPathSegment());
    if( fileUri == null )
    {
      throw new FileNotFoundException("File Id doesn't exist");
    }

    File privateFile = new File(fileUri.getPath());
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
    Utils.printMethodName();
    return UUID.randomUUID().toString() + "__" + UUID.randomUUID().toString();
  }// getRandomFileId

  // ---------------------------------------------------------------------------

  private void copyFile(File from, File to) throws IOException
  {
    Utils.printMethodName();
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
    Utils.printMethodName();
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
        DateFormat.getDateTimeInstance().format(new Date()));

    // update the metadata information
    genericDb.updateFile(file, updateValues);

    return Uri.withAppendedPath(GenericContract.URI_FILES, newId);
  }// newFile

  // ---------------------------------------------------------------------------

  private Uri newBackendFile(ContentValues values)
  {
    Utils.printMethodName();
    genericDb.newFile(values);
    MetaData file = genericDb.getFileMetaData(values
        .getAsString(MetaDataColumns.FILE_ID));
    return Uri.withAppendedPath(GenericContract.URI_FILES, file.fileId());
  }// newBackendFile

  // ---------------------------------------------------------------------------

  private void displayConflictNotification()
  {
    Utils.printMethodName();
    NotificationCompat.Builder mBuilder = new NotificationCompat.Builder(
        getContext()).setContentTitle("MyDesk Conflic")
        .setContentText("Tap to resolve conflict")
        .setSmallIcon(android.R.drawable.stat_notify_sync_noanim);
    // Creates an explicit intent for an Activity in your app
    Intent resultIntent = new Intent(getContext(), ConflictActivity.class);

    // The stack builder object will contain an artificial back stack for the
    // started Activity.
    // This ensures that navigating backward from the Activity leads out of
    // your application to the Home screen.
    TaskStackBuilder stackBuilder = TaskStackBuilder.create(getContext());
    // Adds the back stack for the Intent (but not the Intent itself)
    stackBuilder.addParentStack(ConflictActivity.class);
    // Adds the Intent that starts the Activity to the top of the stack
    stackBuilder.addNextIntent(resultIntent);
    PendingIntent resultPendingIntent = stackBuilder.getPendingIntent(0,
        PendingIntent.FLAG_UPDATE_CURRENT);
    mBuilder.setContentIntent(resultPendingIntent);
    NotificationManager mNotificationManager = (NotificationManager) getContext()
        .getSystemService(Context.NOTIFICATION_SERVICE);
    // mId allows you to update the notification later on.
    mNotificationManager.notify(NOTIFICATION_CONFLICT, mBuilder.build());
  }// displayConflictNotification

  // ---------------------------------------------------------------------------

  private String getUser()
  {
    Utils.printMethodName();
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
    Utils.printMethodName();
    final String queryParam = uri.getQueryParameter(param);
    return queryParam != null && queryParam.equals("true");
  }

}// GenericProvider
