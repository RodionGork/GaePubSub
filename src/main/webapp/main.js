$(document).ready(function() {
    updateTopicList();
    updateSubsList();
});

function updateTopicList() {
    $.getJSON('/demo?q=topics', function(data) {
        var select = $('#topics');
        select.empty();
        for (var i in data.topics) {
            var name = data.topics[i];
            $('<option>' + name + '</option>').attr('value', name).appendTo(select);
        }
    });
}

function updateSubsList() {
    $.getJSON('/demo?q=subs', function(data) {
        var select = $('#subs');
        select.empty();
        for (var i in data.subs) {
            var name = data.subs[i];
            $('<option>' + name + '</option>').attr('value', name).appendTo(select);
        }
    });
}

function publishData() {
    var topic = $('#topics').val();
    var data = $('#data').val();
    $.post('/demo?topic=' + topic, data, function() {alert('Ok!')}, 'text');
}

function fetchData() {
    var sub = $('#subs').val();
    var receiver = $('#receiver');
    receiver.text('Please wait a few seconds...');
    receiver.load('/demo?q=fetch&name=' + sub);
}

function reportResult(data) {
    if (data.ok) {
        alert("SUCCESS!");
    } else {
        alert("ERROR:\n\n" + data.error + "\n\n" + data.message);
    }

}

function createTopic() {
    var name = prompt("New Topic Name");
    if (name == null) {
        return;
    }
    $.getJSON('/demo?q=topic&name=' + name, function(data) {
        reportResult(data);
        updateTopicList();
    });
}

function deleteTopic() {
    var topic = $('#topics').val();
    var ur = confirm("Delete topic '" + topic + "' - are you sure?");
    if (!ur) {
        return;
    }
    $.getJSON('/demo?q=topic-del&name=' + topic, function(data) {
        reportResult(data);
        updateTopicList();
    });
}

function createSub() {
    var topic = $('#topics').val();
    var name = prompt("New Subscription Name (for topic '" + topic + "')");
    if (name == null) {
        return;
    }
    $.getJSON('/demo?q=sub&name=' + name + '&for=' + topic, function (data) {
        reportResult(data);
        updateSubsList();
    });
}

function deleteSub() {
    var name = $('#subs').val();
    var ur = confirm("Delete subscription '" + name + "' - are you sure?");
    if (!ur) {
        return;
    }
    $.getJSON('/demo?q=sub-del&name=' + name, function(data) {
        reportResult(data);
        updateSubsList();
    });
}