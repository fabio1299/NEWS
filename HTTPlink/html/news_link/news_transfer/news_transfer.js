//*****************************************************************************
// Do not remove this notice.
//
// Copyright 2017 University of New Hapmshire by Dr. Alex Prusevich.
// alex.proussevitch@unh.edu
//*****************************************************************************

$(function() {

  var simID		= '',
      CGI	 	= '/cgi-bin/',
      URL_set_status	= '/cgi-bin/set_status.py?',
      renameDialog_tp2m	= $( "#dialog-rename-tp2m" ),
      renameDialog_reeds= $( "#dialog-rename-reeds"),
      listDialog	= $( "#dialog-list" ),
      checkTmOut	= null,
      checkInterval	= 3600*1000,		// in milliseconds
      oldID	  = '',
      selID	  = '';

  $(document).ready(function () {

		// Check status function at load
	get_log('TP2M', 0,0,0);
	get_log('ReEDS',0,0,0);

		// Check status function at interval

	checkTmOut = setInterval( function() {
	  get_log('TP2M', 0,0,0);
	  get_log('ReEDS',0,0,0);

	}, checkInterval);

		// Click functions
	$( "#simulationLog-tp2m" ).on("click", "a.refresh-log", function(event) {
		event.preventDefault();
		get_log('TP2M', 0,0,0);
	});

	$( "#simulationLog-reeds" ).on("click", "a.refresh-log", function(event) {
		event.preventDefault();
		get_log('ReEDS',0,0,0);
	});

	$( "#simulationLog-tp2m" ).on("click", "a.listSet", function(event) {
		event.preventDefault();
		var sid  = $(this).attr('simID');
		get_list('TP2M', sid);
	});

	$( "#simulationLog-reeds" ).on("click", "a.listSet", function(event) {
		event.preventDefault();
		var sid  = $(this).attr('simID');
		get_list('ReEDS', sid);
	});

	$( "#simulationLog-tp2m" ).on("click", "a.moveSet", function(event) {
		event.preventDefault();
		var sid  = $(this).attr('simID');
		var move = $(this).attr('action');
		get_log('TP2M',0, sid, move);
	});

	$( "#simulationLog-reeds" ).on("click", "a.moveSet", function(event) {
		event.preventDefault();
		var sid  = $(this).attr('simID');
		var move = $(this).attr('action');
		get_log('ReEDS',0, sid, move);
	});

	$( "#simulationLog-tp2m" ).on("click", "a.renameSet", function(event) {
		event.preventDefault();
		oldID = $(this).attr('simID');
		renameDialog_tp2m.dialog( "open" );
	});

	$( "#simulationLog-reeds" ).on("click", "a.renameSet", function(event) {
		event.preventDefault();
		oldID = $(this).attr('simID');
		renameDialog_reeds.dialog( "open" );
	});

        $( "#simulationLog-tp2m" ).on("click", "a.removeSet", function(event) {
		var sid = $(this).attr('simID');
		if (confirm("Please, confirm to remove this dataset-\n\""+sid+"\"")) {
			get_log('TP2M',sid,0,0);
		}
	});

	$( "#simulationLog-reeds" ).on("click", "a.removeSet", function(event) {
		event.preventDefault();
		var sid = $(this).attr('simID');
		if (confirm("Please, confirm to remove this dataset-\n\""+sid+"\"")) {
			get_log('ReEDS',sid,0,0);
		}
	});

  });

  //////////	Rename simulation dialog
  renameDialog_tp2m.dialog({
	autoOpen: false,
	modal: true,
	buttons: {
	  Rename: function() {
		var msg = 'Input for simulation ID ';
		var newID = $( "#newname-tp2m" ).val();
		newID = validateString($( "#controls input[name=simID]" ),'New ID ',newID,4,24);
		if (newID) {
			get_log( 'TP2M', oldID, newID, 0 );
			$( "#newname-tp2m" ).val('');
			$( this ).dialog( "close" );
		}
	  },
	  Cancel: function() {
		$( "#newname" ).val('');
		$( this ).dialog( "close" );
	  }
	}
  });

  renameDialog_reeds.dialog({
	autoOpen: false,
	modal: true,
	buttons: {
	  Rename: function() {
		var msg = 'Input for simulation ID ';
		var newID = $( "#newname-reeds" ).val();
		newID = validateString($( "#controls input[name=simID]" ),'New ID ',newID,4,24);
		if (newID) {
			get_log( 'ReEDS', oldID, newID, 0 );
			$( "#newname-reeds" ).val('');
			$( this ).dialog( "close" );
		}
	  },
	  Cancel: function() {
		$( "#newname" ).val('');
		$( this ).dialog( "close" );
	  }
	}
  });

  function validateString(inpt,msg,val,min,max) {
	if (!val.length) {
		alert(msg + 'is empty');
		inpt.focus(); return false;
	}
	val = val.replace(/\s/g,'_');		// replace spaces
	if (!val.match(/^[\w-\.]+$/i)) {
		alert(msg + 'is not alpha-numeric');
                inpt.focus(); return false;
	}
	if (val.length < min || val.length > max) {
		alert(msg + 'is outside of allowed length range ('+min+','+max+')');
		inpt.focus(); return false;
	}
	return val;
  }

  //////////	File List dialog
  listDialog.dialog({
	autoOpen: false,
	width: 520,
	modal: true,
	buttons: {
	  Close: function() {
		$( this ).dialog( "close" );
	  }
	}
  });

  /////////////////////////////////////
  //////////	Log Table Functions

  function get_log(rSet,remID,newID,move) {
    var url = CGI+'get_log.pl?set='+rSet+'&remove='+remID+'&newname='+newID+'&move='+move;
    $.get( url, function(data) {
	$( "#simulationLog-"+rSet.toLowerCase() ).html(data);
	if (newID || move) {
		if (newID) selID = newID;
		$( 'td[class="cell_normal_blue '+selID+'"]').attr('class','cell_normal_blue_sel '+selID);
		$( 'td[class="cell_normal_tan ' +selID+'"]').attr('class','cell_normal_tan_sel ' +selID);
	}
    });
  }

  function get_list(rSet,setID) {
    var url = CGI+'get_log.pl?set='+rSet+'&list='+setID;
    $.get( url, function(data) {
	listDialog.html(data);
	listDialog.dialog('option', 'title', 'List of Files in '+rSet+'/'+setID+'/');;
	listDialog.dialog( "open" );
		$( 'td[class="cell_normal_blue_sel '+selID+'"]').attr('class','cell_normal_blue ' +selID);
		$( 'td[class="cell_normal_tan_sel ' +selID+'"]').attr('class','cell_normal_tan '  +selID);
		selID = setID;
		$( 'td[class="cell_normal_blue '+selID+'"]').attr('class','cell_normal_blue_sel '+selID);
		$( 'td[class="cell_normal_tan ' +selID+'"]').attr('class','cell_normal_tan_sel ' +selID);
    });
  }

});
