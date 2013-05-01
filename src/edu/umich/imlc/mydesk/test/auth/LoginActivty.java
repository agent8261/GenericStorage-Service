package edu.umich.imlc.mydesk.test.auth;

import edu.umich.imlc.mydesk.cloud.android.auth.LoginCallback;
import edu.umich.imlc.mydesk.cloud.android.auth.LoginUtilities;
import edu.umich.imlc.mydesk.test.common.GenericContract;
import android.accounts.Account;
import android.app.Activity;
import android.app.AlertDialog;
import android.app.ProgressDialog;
import android.content.ContentResolver;
import android.content.DialogInterface;
import android.content.DialogInterface.OnDismissListener;
import android.content.Intent;
import android.content.SharedPreferences;

public class LoginActivty extends Activity implements LoginCallback
{
  ProgressDialog prog;
  SharedPreferences prefs;

  /*
   * (non-Javadoc)
   * 
   * @see android.app.Activity#onActivityResult(int, int,
   * android.content.Intent)
   */
  @Override
  protected void onActivityResult(int requestCode, int resultCode, Intent data)
  {
    if( (resultCode == Activity.RESULT_OK) )
    {
      prog = new ProgressDialog(this);
      prog.setCancelable(false);
      prog.setTitle("Verifying Account");
      prog.show();
      LoginUtilities.doOnActivityResult(this, requestCode, resultCode, data,
          this);
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see android.app.Activity#onStart()
   */
  @Override
  protected void onStart()
  {
    super.onStart();
    if( getSharedPrefs().contains(GenericContract.PREFS_ACCOUNT_NAME) )
    {
      // account verified
      showAccountVerified(getSharedPrefs().getString(
          GenericContract.PREFS_ACCOUNT_NAME, ""));
      return;
    }
    LoginUtilities.startAccountPicker(this, true);
  }

  private SharedPreferences getSharedPrefs()
  {
    if( prefs == null )
    {
      prefs = getSharedPreferences(GenericContract.SHARED_PREFS, MODE_PRIVATE);
    }
    return prefs;
  }

  @Override
  public void onSuccess(final String accountName)
  {
    getSharedPrefs().edit()
        .putString(GenericContract.PREFS_ACCOUNT_NAME, accountName).apply();
    Account account = new Account(accountName, LoginUtilities.googleAccountType);
    ContentResolver.setIsSyncable(account, GenericContract.AUTHORITY, 1);
    ContentResolver.setSyncAutomatically(account, GenericContract.AUTHORITY, true);
    runOnUiThread(new Runnable()
    {
      
      @Override
      public void run()
      {
        prog.dismiss();
        showAccountVerified(accountName);   
      }
    });
  }

  @Override
  public void onFailure(final Exception e)
  {
    e.printStackTrace();
    runOnUiThread(new Runnable()
    {
      @Override
      public void run()
      {
        prog.dismiss();
        showVerifyFail(e); 
      }
    });

  }

  private void showAccountVerified(String accountName)
  {
    AlertDialog.Builder aBuilder = new AlertDialog.Builder(this);
    aBuilder.setMessage("Account verified: " + accountName);
    aBuilder.setOnDismissListener(new OnDismissListener()
    {
      @Override
      public void onDismiss(DialogInterface dialog)
      {
        finish();
      }
    });
    aBuilder.create().show();
  }
  
  private void showVerifyFail(Throwable e)
  {
    AlertDialog.Builder aBuilder = new AlertDialog.Builder(this);
    aBuilder.setMessage("Verify failed: " + e);
    aBuilder.setOnDismissListener(new OnDismissListener()
    {
      @Override
      public void onDismiss(DialogInterface dialog)
      {
        finish();
      }
    });
    aBuilder.create().show();
  }
}
