//*****************************************************************************
// Do not remove this notice.
//
// Copyright 2016 University of New Hapmshire by Dr. Alex Prusevich.
// alex.proussevitch@unh.edu
//*****************************************************************************

$(function() {

  var simID		= '',
      URL_chk_tp2m	= '/status_tp2m.txt',
      URL_chk_reeds	= '/status_reeds.txt',
      URL_chk_n_tp2m	= '/counter_tp2m.txt',
      URL_chk_n_reeds	= '/counter_reeds.txt',
      URL_set_status	= '/cgi-bin/set_status.py?',
      block_row		= $( "#block_row" ),
      block_text1	= $( "#monitoring1" ),
      block_text2	= $( "#monitoring2" ),
      chck_block	= $( "#block_chkbox" ),
      noteDialog	= $( "#dialog-note" ),
      checkTmOut	= null,
      checkInterval	= 5000;				// in milliseconds

  $(document).ready(function () {

		// Check status function at load

			// TP2M status
	$.get( URL_chk_tp2m,    function(data) { $("#status_tp2m").html(data); }).fail(function(){return false;});
	$.get( URL_chk_n_tp2m,  function(data) { $("#counter_tp2m").html(data); }).fail(function(){return false;});
			// ReEDS status
	$.get( URL_chk_reeds,   function(data) { $("#status_reeds").html(data); }).fail(function(){return false;});
	$.get( URL_chk_n_reeds, function(data) { $("#counter_reeds").html(data); }).fail(function(){return false;});

		// Check status function at interval

	checkTmOut = setInterval( function() {
	  var rid = '?' + Math.random().toString().substr(2,7);		// random ID
			// TP2M status
	  $.get( URL_chk_tp2m+rid,    function(data) { $("#status_tp2m").html(data); }).fail(function(){return false;});
	  $.get( URL_chk_n_tp2m+rid,  function(data) { $("#counter_tp2m").html(data); }).fail(function(){return false;});
			// ReEDS status
	  $.get( URL_chk_reeds+rid,   function(data) { $("#status_reeds").html(data); }).fail(function(){return false;});
	  $.get( URL_chk_n_reeds+rid, function(data) { $("#counter_reeds").html(data); }).fail(function(){return false;});

	}, checkInterval);
  });

		// Click functions

  $("#run_first_tp2m").click(function() {
	if (check_block()) return false;
	var url_s = URL_set_status + 'file=status_tp2m.txt&status=starting';
	var url_c = URL_set_status + 'file=counter_tp2m.txt&status=0';
	$.get( url_s, function(data) {return false;}).fail(function(){return false;});
	$.get( url_c, function(data) {return false;}).fail(function(){return false;});
	$("#status_tp2m").html('starting');
	$("#counter_tp2m").html('0');
  });
  $("#run_second_tp2m").click(function() {
	if (check_block()) return false;
	var url = URL_set_status + 'file=status_tp2m.txt&status=waiting';
	$.get( url, function(data) {return false;}).fail(function(){return false;});
	$("#status_tp2m").html('waiting');
  });
  $("#stop_tp2m").click(function() {
	if (check_block()) return false;
	var url = URL_set_status + 'file=status_tp2m.txt&status=stopped';
	$.get( url, function(data) {return false;}).fail(function(){return false;});
	$("#status_tp2m").html('stopped');
  });
  $("#run_first_reeds").click(function() {
	if (check_block()) return false;
	var url_s = URL_set_status + 'file=status_reeds.txt&status=starting';
	var url_c = URL_set_status + 'file=counter_reeds.txt&status=0';
	$.get( url_s, function(data) {return false;}).fail(function(){return false;});
	$.get( url_c, function(data) {return false;}).fail(function(){return false;});
	$("#status_reeds").html('starting');
	$("#counter_reeds").html('0');
  });
  $("#run_second_reeds").click(function() {
	if (check_block()) return false;
	var url = URL_set_status + 'file=status_reeds.txt&status=waiting';
	$.get( url, function(data) {return false;}).fail(function(){return false;});
	$("#status_reeds").html('waiting');
  });
  $("#stop_reeds").click(function() {
	if (check_block()) return false;
	var url = URL_set_status + 'file=status_reeds.txt&status=stopped';
	$.get( url, function(data) {return false;}).fail(function(){return false;});
	$("#status_reeds").html('stopped');
  });
  chck_block.change(function() {
	if (chck_block.prop('checked')) {
		block_row.css("background-color","Pink");
		block_text1.html('In Monitoring mode now!<br />');
		block_text2.html('Uncheck to disable "Monitoring" mode by unblocking action buttons.');
	}
	else {
		block_row.css("background-color","PaleGreen");
		block_text1.html('In Action mode now!<br />');
		block_text2.html('Check to enable "Monitoring" mode by blocking action buttons.');
	}
  });

  function check_block() {
    if (chck_block.prop('checked')) {
      noteDialog.html('Console is in "Monitoring" mode.<br \>Uncheck it to enable controls.');
      noteDialog.dialog( "open" );
      return true;
    }
    return false;
  }

  //////////	Message dialog
  noteDialog.dialog({
	autoOpen: false,
	width: 320,
	modal: true,
	buttons: {
	  Close: function() {
		$( this ).dialog( "close" );
	  }
	}
  });

});
