String.prototype.endsWith = function(suffix) {
    return this.indexOf(suffix, this.length - suffix.length) !== -1;
};


function console() {
	var _ = this;


	// this.user = "client";
	// this.pass = "client"

	this.job_count=1;
	this.tmp_job_id =-1;

	function do_updates() {
		update();
		setTimeout( do_updates, 1500 )
	}

	function node_id(n) {
		return (n.host + "-" + n.port).replace(/\./g, '_');
	} 

	function job_id(j) {
		return "job_" + j.job_id;
	}
	function work_id(j, i) {
		return "work_unit_" + j.job_id + "_" + i;
	}
	function node_work_id(j, i) {
		return "node_work_unit_" + j + "_" + i;
	}
	function file_id(j, i) {
		return "file_" + j.job_id + "_" + i;
	}

	function node_name(n) {
		return n.host + ":" + n.port;
	} 

	function headers(){

		return {
			// "Authorization": "Basic " + Base64.encode(_.user +":" + _.pass)
		};

	}

	function update_cpu(li, new_cpu){
		cpuh = li.data("cpu-history");

		// Update the list
		cpuh=cpuh.splice(1, cpuh.length-1);
		cpuh.push(new_cpu);
		li.data("cpu-history", cpuh);


		max_height =  parseFloat(li.find("div.cpu-chart").height());
		bars = li.find("div.bar");
		for (i =0; i < cpuh.length; i++){
			h = parseInt(cpuh[i] / (100 / max_height));
			$(bars[i]).css("height", h);
			$(bars[i]).css("margin-top", max_height - h );

		}

	}

	function render_nodes(updates) {
		$("#nodes li").addClass("remove");

		for (var i = 0; i < updates.length; i++) {
			// Check to see if we already have a node here?
			var n = updates[i];


			var li = $("#" + node_id(n) );
			if ( li.length==0){
				// Create a new list item for you please!
				li = $("#node-template").clone();
				li.attr("id", node_id(n));
				$("#nodes").append( li );

				var cpuh = [];
				for (var i=0; i < 60; i++){
					bar = $("<div>");
					bar.addClass("bar");
					li.find("div.cpu-chart").append(bar)
					cpuh.push(3);
					li.data("cpu-history", cpuh);
				}



			}

			li.removeClass("remove");
			li.find(".node-name").text( n.node_ident + " [" + n.node_id + "]");
			li.find(".node-type").text( n.type);
			li.find(".node-cpu").text( parseInt(n.cpu) + "%" );
			li.find(".node-cost").text( "$"+ (parseFloat(n.cost)/100).toFixed(2) );

			update_cpu(li, parseInt(n.cpu));

			// If its here, then its online YO!
			li.find(".node-status-led").attr("src", "/console/img/ledgreen.png");
			

			if (n.work_units!=null){
				for (ii=0; ii < n.work_units.length; ii++){
					w = n.work_units[ii]

					wid = work_id(i, ii);

					createWorkUnit(w, wid, li.find(".workunits"))
				}
			}

		}
		$("#nodes li.remove").remove()
	}

	function createWorkUnit(w, wid, ul){
		wt = $("#" + wid);
		if (wt.length==0){
		wt = $("#work-template").clone();
			wt.attr("id", wid);
			ul.append(wt);
		}
		wt.removeClass("remove");

		// Now actually set the Work Unit information...
		if (w.kill_msg != null && w.kill_msg.length > 0){
			wt.find(".work-status").text(w.kill_msg);

		}else{
			wt.find(".work-status").text(w.status);
		}
		wt.find(".work-file").text(w.filename);

		wtled = wt.find("img.unit-status-led");

		if (w.status=="PENDING") wtled.attr("src", "/console/img/ledblue.png")
		if (w.status=="READY") wtled.attr("src", "/console/img/ledblue.png")
		if (w.status=="RUNNING") wtled.attr("src", "/console/img/ledpurple.png")
		if (w.status=="FINISHED") wtled.attr("src", "/console/img/ledgreen.png")
		if (w.status=="QUEUED") wtled.attr("src", "/console/img/ledorange.png")
		if (w.status=="KILLED") wtled.attr("src", "/console/img/ledred.png")
	}

	function render_logs(updates){
		ul = $("#log");
		for (i=0; i< updates.length; i++){
			l = updates[i];

			var li = $("#log-entry-" + l.id );

			// You are new!
			if (li.length==0){
				li = $("<li>").attr("id", "log-entry-" + l.id );
				li.text(l.log.toUpperCase());
				ul.prepend(li);
			}

		}
	} 

	function render_jobs(updates) {
		$("#jobs li").addClass("remove");

		for (var i = 0; i < updates.length; i++) {
			// Check to see if we already have a node here?
			var j = updates[i];

			// alert(j.job_id);

			var li = $("#" + job_id(j) );
			if ( li.length==0){
				// Create a new list item for you please!
				li = $("#job-template").clone();
				li.attr("id", job_id(j));
				$("#jobs").prepend( li );


				// Bind the cancel button
				var cancel = li.find("button.cancel");
				cancel.data("job", j);
				cancel.click(function(){
					j = cancel.data("job");
					$.ajax({
						url: "/job/" + j.job_id,
						type: "DELETE",
						success: function (response) {
							alert("Canceled Job");
							cancel.hide();
						},
						error: function () {
						}
					});

				});
			}

			li.removeClass("remove");
			li.find(".job-name").text(j.name + " [" + j.job_id + "]");
			if (j.kill_msg != null){
				li.find(".job-status").text( j.kill_msg );

			}else{
				li.find(".job-status").text( j.status );
			}

			li.find(".job-command").text( j.executable );

			led = li.find("img.job-status-led");

			if (j.status=="PENDING") led.attr("src", "/console/img/ledblue.png");
			if (j.status=="READY") led.attr("src", "/console/img/ledblue.png");
			if (j.status=="QUEUED") led.attr("src", "/console/img/ledorange.png");
			if (j.status=="RUNNING") led.attr("src", "/console/img/ledpurple.png");
			if (j.status=="FINISHED") led.attr("src", "/console/img/ledgreen.png");
			if (j.status=="KILLED") led.attr("src", "/console/img/ledred.png");

			if (j.status=="PENDING") li.find("button.cancel").show();
			if (j.status=="READY") li.find("button.cancel").show();
			if (j.status=="QUEUED") li.find("button.cancel").show();
			if (j.status=="RUNNING") li.find("button.cancel").show();
			if (j.status=="FINISHED") li.find("button.cancel").hide();
			if (j.status=="KILLED") li.find("button.cancel").hide();


			if (j.work_units!=null){
				for (ii=0; ii < j.work_units.length; ii++){
					w = j.work_units[ii]

					wid = work_id(j, ii);

					createWorkUnit(w, wid, li.find(".workunits"))

				}
			}
			if (j.files != null){
				for (ii=0; ii < j.files.length; ii++){
					f = j.files[ii]



					if (f.endsWith(".o")){
						var stdout = $("#work_unit_" + f.substring(0, f.length-2) + " a.stdout");
						if (stdout!=null){
							stdout.attr("href", "/job/"+j.job_id+"/output/"+f)

							$("#work_unit_" + f.substring(0, f.length-2) + " .output").css("display", "block");
						}
					}
					if (f.endsWith(".e")){

						var stderr = $("#work_unit_" + f.substring(0, f.length-2) + " a.stderr");
						if (stderr!=null){
							stderr.attr("href", "/job/"+j.job_id+"/output/"+f)
							$("#work_unit_" + f.substring(0, f.length-2) + " .output").css("display", "block");
						}
					}

				}

			}


		}
		$("#jobs li.remove").remove()
	}

	function update() {
		$.ajax({
			url: "/json/nodes",
			success: function (response) {
				render_nodes(response.nodes);
			},
			error: function () {
			}
		});


		$.ajax({
			url: "/json/jobs",
			success: function (response) {
				render_jobs(response.jobs);
			},
			error: function () {
			}
		});

		$.ajax({
			url: "/json/logs",
			success: function (response) {
				render_logs(response.log);
			},
			error: function () {
			}
		});
	}

	do_updates();

	// Stuff for handling job submission

	$("#register").click( function() {
		var txt = $("#text-job-name");
		var cmd = $("#text-job-cmd");


		if (txt.val().indexOf("/") > -1){
			alert("Your name may not include a '/' character")
		}

		if (txt.val().length > 0) {
			update_job(txt.val(), cmd.val())

		}


	});

	$("#enqueue").click(function() {

		$.ajax({
			url: "/job/"+_.tmp_job_id+"/status",
			type: "PUT",
			headers: headers(),
			data: JSON.stringify({ 'status': 'READY' }),
			success: function (response) {

				// Reset the form and hide everything that needs hiding
				_.tmp_job_id = -1

				_.job_count++;

				resetForm();

				// alert("Your job has been queued with JobId:" + response.job_id)
			},
			error: function (response) {
				alert("Error starting job: " + JSON.parse(response.responseText).error_msg);
				resetForm();
			}
		});

	});

	function update_job(name, command){

		$.ajax({
			url: "/job",
			type: "POST",
			headers: headers(),
            dataType: 'json',
			data: JSON.stringify({	name: $("#text-job-name").val(), 
					wall_time: $("#text-wall-time").val(), 
					deadline: $("#text-deadline").val(), 
					// deadline: new Date($("#text-deadline").val()).getTime()/1000, 
					budget: parseFloat($("#text-budget").val())*100, 
					job_type: $("#text-job-type").val(), 
					flags : ""}),
			success: function (response) {
				if (_.tmp_job_id == -1){

					_.tmp_job_id = response.id;
					_.job_count ++;

					// Create the uploaders now so they point to the correct job id.
					create_executable_uploader(_.tmp_job_id);
					create_input_uploader(_.tmp_job_id);



					$(".step-define").removeClass("highlighted");
					$(".step-upload").addClass("highlighted");

					// Hide the "Register" screen and show the "Upload" screen
					$("tbody.step-define").css("display", "none");
					$("tbody.step-upload").css("display", "");


				}
			},
			error: function (response) {
				alert("Error registering job: " + JSON.parse(response.responseText).error_msg);
			}
		});

	}

	function resetForm(){

		// Swap the highlight on the current step
		$(".step-define").addClass("highlighted");
		$(".step-upload").removeClass("highlighted");

		// Swap pages
		$("tbody.step-define").css("display", "");
		$("tbody.step-upload").css("display", "none");

		// Reset the form...
		$("#text-job-name").val("New job " + _.job_count)
		$("#text-deadline").val( formatDate(d) );
		$("#text-wall-time").val( "1:00:00" );
		$("#text-budget").val( "100" );
	}



	function create_executable_uploader(tmpid){
		_.executable_uploader = new qq.FileUploader({
			element: document.getElementById("executable-files"),
			action: '/json/job/submit-executable/'+tmpid+"/",
			onComplete: function (id, fileName, responseJSON) {
				$("#enqueue").css("display", "block");
				return true;
			},
			onSubmit: function (id, fileName) {
				// $("ul.qq-upload-list").addClass("visible")
				return true;
			},

			onFileDraggedOver: function (isActive) {
				$("#drop-target").css("display", "block");
			},
			onCancel: function () {
				alert("canceled");
			}

		});


		$(".row-executable-files .qq-upload-drop-area span").text("Drop EXECUTABLE here to upload");

	}

	function create_input_uploader(tmpid){
		_.input_uploader = new qq.FileUploader({
			element: document.getElementById("input-files"),
			action: '/json/job/submit-file/'+tmpid+"/",
			multiple: false,
			onComplete: function (id, fileName, responseJSON) {
				$("#enqueue").css("display", "block");
				return true;
			},
			onSubmit: function (id, fileName) {
				// $("ul.qq-upload-list").addClass("visible")
				return true;
			},
			// onDrop: function(e){
			// 	if (e.dataTransfer.files.length > 1){
			// 		alert("Only one executable file can be added")
			// 	} else{
			// 		self._uploadFileList(e.dataTransfer.files);  
			// 	}
			// }

			onFileDraggedOver: function (isActive) {
				$("#drop-target").css("display", "block");
			},
			onCancel: function () {
				alert("canceled");
			}

		});
		$(".row-input-files .qq-upload-drop-area span").text("Drop INPUT here to upload");

	}


	function format(num){
		if (num < 10){
			return "0" + num;
		}else{
			return "" + num;
		}
	}
	function formatDate(){

		month = d.getMonth() + 1;
		day = d.getDate()+1;
		if (day==32){
			month++;
			day=1
		}

		return d.getFullYear() +
			"-" + format(month) +
			"-" + format(day) +
			" " + format(d.getHours()) +
			":" + format(d.getMinutes()) +
			":" + format(d.getSeconds())
 
	}


	function init(){

		d = new Date();


		$("#text-job-name").val("New job " + _.job_count)
		$("#text-deadline").val( formatDate(d) );
	}

	init();


}
