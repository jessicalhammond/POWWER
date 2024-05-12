import React, { Component, PropTypes } from 'react';
import { Panel } from 'react-bootstrap';

const title = 'Icons';

class Icons extends Component {
  static contextTypes = {
    setTitle: PropTypes.func.isRequired,
  };

  componentWillMount() {
    this.context.setTitle(title);
  }

  render() {
    return (
      <div className="animate">
        <div className="row">
          <div className="col-sm-12">

            <Panel
              header={<span>Icons</span>}
              bsStyle="primary"
            >

              <div className="row">
                <div className="fa col-lg-3">
                  <p><i className="fa fa-glass" />
        &nbsp;												fa-glass
                  </p><p><i className="fa fa-music" />
        &nbsp;												fa-music
                  </p><p><i className="fa fa-search" />
        &nbsp;											fa-search
                  </p><p><i className="fa fa-envelope-o" />
        &nbsp;												fa-envelope-o
                  </p><p><i className="fa fa-heart" />
        &nbsp;												fa-heart
                  </p><p><i className="fa fa-star" />
        &nbsp;												fa-star
                  </p><p><i className="fa fa-star-o" />
        &nbsp;												fa-star-o
                  </p><p><i className="fa fa-user" />
        &nbsp;												fa-user
                  </p><p><i className="fa fa-film" />
        &nbsp;												fa-film
                  </p><p><i className="fa fa-th-large" />
        &nbsp;												fa-th-large
                  </p><p><i className="fa fa-th" />
        &nbsp;												fa-th
                  </p><p><i className="fa fa-th-list" />
        &nbsp;												fa-th-list
                  </p><p><i className="fa fa-check" />
        &nbsp;												fa-check
                  </p><p><i className="fa fa-times" />
        &nbsp;												fa-times
                  </p><p><i className="fa fa-search-plus" />
        &nbsp;												fa-search-plus
                  </p><p><i className="fa fa-search-minus" />
        &nbsp;												fa-search-minus
                  </p><p><i className="fa fa-power-off" />
        &nbsp;												fa-power-off
                  </p><p><i className="fa fa-signal" />
        &nbsp;												fa-signal
                  </p><p><i className="fa fa-gear" />
        &nbsp;												fa-gear
                  </p><p><i className="fa fa-cog" />
        &nbsp;												fa-cog
                  </p><p><i className="fa fa-trash-o" />
        &nbsp;												fa-trash-o
                  </p><p><i className="fa fa-home" />
        &nbsp;												fa-home
                  </p><p><i className="fa fa-file-o" />
        &nbsp;												fa-file-o
                  </p><p><i className="fa fa-clock-o" />
        &nbsp;												fa-clock-o
                  </p><p><i className="fa fa-road" />
        &nbsp;												fa-road
                  </p><p><i className="fa fa-download" />
        &nbsp;												fa-download
                  </p><p><i className="fa fa-arrow-circle-o-down" />
        &nbsp;												fa-arrow-circle-o-down
                  </p><p><i className="fa fa-arrow-circle-o-up" />
        &nbsp;												fa-arrow-circle-o-up
                  </p><p><i className="fa fa-inbox" />
        &nbsp;												fa-inbox
                  </p><p><i className="fa fa-play-circle-o" />
        &nbsp;												fa-play-circle-o
                  </p><p><i className="fa fa-rotate-right" />
        &nbsp;												fa-rotate-right
                  </p><p><i className="fa fa-repeat" />
        &nbsp;												fa-repeat
                  </p><p><i className="fa fa-refresh" />
        &nbsp;												fa-refresh
                  </p><p><i className="fa fa-list-alt" />
        &nbsp;												fa-list-alt
                  </p><p><i className="fa fa-lock" />
        &nbsp;												fa-lock
                  </p><p><i className="fa fa-flag" />
        &nbsp;												fa-flag
                  </p><p><i className="fa fa-headphones" />
        &nbsp;												fa-headphones
                  </p><p><i className="fa fa-volume-off" />
        &nbsp;												fa-volume-off
                  </p><p><i className="fa fa-volume-down" />
        &nbsp;												fa-volume-down
                  </p><p><i className="fa fa-volume-up" />
        &nbsp;												fa-volume-up
                  </p><p><i className="fa fa-qrcode" />
        &nbsp;												fa-qrcode
                  </p><p><i className="fa fa-barcode" />
        &nbsp;												fa-barcode
                  </p><p><i className="fa fa-tag" />
        &nbsp;												fa-tag
                  </p><p><i className="fa fa-tags" />
        &nbsp;												fa-tags
                  </p><p><i className="fa fa-book" />
        &nbsp;												fa-book
                  </p><p><i className="fa fa-bookmark" />
        &nbsp;												fa-bookmark
                  </p><p><i className="fa fa-print" />
        &nbsp;												fa-print
                  </p><p><i className="fa fa-camera" />
        &nbsp;												fa-camera
                  </p><p><i className="fa fa-font" />
        &nbsp;												fa-font
                  </p><p><i className="fa fa-bold" />
        &nbsp;												fa-bold
                  </p><p><i className="fa fa-italic" />
        &nbsp;												fa-italic
                  </p><p><i className="fa fa-text-height" />
        &nbsp;												fa-text-height
                  </p><p><i className="fa fa-text-width" />
        &nbsp;												fa-text-width
                  </p><p><i className="fa fa-align-left" />
        &nbsp;												fa-align-left
                  </p><p><i className="fa fa-align-center" />
        &nbsp;												fa-align-center
                  </p><p><i className="fa fa-align-right" />
        &nbsp;												fa-align-right
                  </p><p><i className="fa fa-align-justify" />
        &nbsp;												fa-align-justify
                  </p><p><i className="fa fa-list" />
        &nbsp;												fa-list
                  </p><p><i className="fa fa-dedent" />
        &nbsp;												fa-dedent
                  </p><p><i className="fa fa-outdent" />
        &nbsp;												fa-outdent
                  </p><p><i className="fa fa-indent" />
        &nbsp;												fa-indent
                  </p><p><i className="fa fa-video-camera" />
        &nbsp;												fa-video-camera
                  </p><p><i className="fa fa-photo" />
        &nbsp;												fa-photo
                  </p><p><i className="fa fa-image" />
        &nbsp;												fa-image
                  </p><p><i className="fa fa-picture-o" />
        &nbsp;												fa-picture-o
                  </p><p><i className="fa fa-pencil" />
        &nbsp;												fa-pencil
                  </p><p><i className="fa fa-map-marker" />
        &nbsp;												fa-map-marker
                  </p><p><i className="fa fa-adjust" />
        &nbsp;												fa-adjust
                  </p><p><i className="fa fa-tint" />
        &nbsp;												fa-tint
                  </p><p><i className="fa fa-edit" />
        &nbsp;												fa-edit
                  </p><p><i className="fa fa-pencil-square-o" />
        &nbsp;												fa-pencil-square-o
                  </p><p><i className="fa fa-share-square-o" />
        &nbsp;												fa-share-square-o
                  </p><p><i className="fa fa-check-square-o" />
        &nbsp;												fa-check-square-o
                  </p><p><i className="fa fa-arrows" />
        &nbsp;												fa-arrows
                  </p><p><i className="fa fa-step-backward" />
        &nbsp;												fa-step-backward
                  </p><p><i className="fa fa-fast-backward" />
        &nbsp;												fa-fast-backward
                  </p><p><i className="fa fa-backward" />
        &nbsp;												fa-backward
                  </p><p><i className="fa fa-play" />
        &nbsp;												fa-play
                  </p><p><i className="fa fa-pause" />
        &nbsp;												fa-pause
                  </p><p><i className="fa fa-stop" />
        &nbsp;												fa-stop
                  </p><p><i className="fa fa-forward" />
        &nbsp;												fa-forward
                  </p><p><i className="fa fa-fast-forward" />
        &nbsp;												fa-fast-forward
                  </p><p><i className="fa fa-step-forward" />
        &nbsp;												fa-step-forward
                  </p><p><i className="fa fa-eject" />
        &nbsp;												fa-eject
                  </p><p><i className="fa fa-chevron-left" />
        &nbsp;												fa-chevron-left
                  </p><p><i className="fa fa-chevron-right" />
        &nbsp;												fa-chevron-right
                  </p><p><i className="fa fa-plus-circle" />
        &nbsp;												fa-plus-circle
                  </p><p><i className="fa fa-minus-circle" />
        &nbsp;												fa-minus-circle
                  </p><p><i className="fa fa-times-circle" />
        &nbsp;												fa-times-circle
                  </p><p><i className="fa fa-check-circle" />
        &nbsp;												fa-check-circle
                  </p><p><i className="fa fa-question-circle" />
        &nbsp;												fa-question-circle
                  </p><p><i className="fa fa-info-circle" />
        &nbsp;												fa-info-circle
                  </p><p><i className="fa fa-crosshairs" />
        &nbsp;												fa-crosshairs
                  </p><p><i className="fa fa-times-circle-o" />
        &nbsp;												fa-times-circle-o
                  </p><p><i className="fa fa-check-circle-o" />
        &nbsp;												fa-check-circle-o
                  </p><p><i className="fa fa-ban" />
        &nbsp;												fa-ban
                  </p><p><i className="fa fa-arrow-left" />
        &nbsp;												fa-arrow-left
                  </p><p><i className="fa fa-arrow-right" />
        &nbsp;												fa-arrow-right
                  </p><p><i className="fa fa-arrow-up" />
        &nbsp;												fa-arrow-up
                  </p><p><i className="fa fa-arrow-down" />
        &nbsp;												fa-arrow-down
                  </p><p><i className="fa fa-mail-forward" />
        &nbsp;												fa-mail-forward
                  </p><p><i className="fa fa-share" />
        &nbsp;												fa-share
                  </p><p><i className="fa fa-expand" />
        &nbsp;												fa-expand
                  </p><p><i className="fa fa-compress" />
        &nbsp;												fa-compress
                  </p><p><i className="fa fa-plus" />
        &nbsp;												fa-plus
                  </p><p><i className="fa fa-minus" />
        &nbsp;												fa-minus
                  </p><p><i className="fa fa-asterisk" />
        &nbsp;												fa-asterisk
                  </p><p><i className="fa fa-exclamation-circle" />
        &nbsp;												fa-exclamation-circle
                  </p><p><i className="fa fa-gift" />
        &nbsp;												fa-gift
                  </p><p><i className="fa fa-leaf" />
        &nbsp;												fa-leaf
                  </p><p><i className="fa fa-fire" />
        &nbsp;												fa-fire
                  </p><p><i className="fa fa-eye" />
        &nbsp;												fa-eye
                  </p><p><i className="fa fa-eye-slash" />
        &nbsp;												fa-eye-slash
                  </p><p><i className="fa fa-warning" />
        &nbsp;												fa-warning
                  </p><p><i className="fa fa-exclamation-triangle" />
        &nbsp;												fa-exclamation-triangle
                  </p><p><i className="fa fa-plane" />
        &nbsp;												fa-plane
                  </p><p><i className="fa fa-calendar" />
        &nbsp;												fa-calendar
                  </p><p><i className="fa fa-random" />
        &nbsp;												fa-random
                  </p><p><i className="fa fa-comment" />
        &nbsp;												fa-comment
                  </p><p><i className="fa fa-magnet" />
        &nbsp;												fa-magnet
                  </p><p><i className="fa fa-chevron-up" />
        &nbsp;												fa-chevron-up
                  </p><p><i className="fa fa-chevron-down" />
        &nbsp;												fa-chevron-down
                  </p><p><i className="fa fa-retweet" />
        &nbsp;												fa-retweet
                  </p><p><i className="fa fa-shopping-cart" />
        &nbsp;												fa-shopping-cart
                  </p><p><i className="fa fa-folder" />
        &nbsp;												fa-folder
                  </p><p><i className="fa fa-folder-open" />
        &nbsp;												fa-folder-open                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      </p>
                </div>
                <div className="fa col-lg-3">
                  <p><i className="fa fa-arrows-h" />
        &nbsp;												fa-arrows-h
                  </p><p><i className="fa fa-bar-chart-o" />
        &nbsp;												fa-bar-chart-o
                  </p><p><i className="fa fa-twitter-square" />
        &nbsp;												fa-twitter-square
                  </p><p><i className="fa fa-facebook-square" />
        &nbsp;												fa-facebook-square
                  </p><p><i className="fa fa-camera-retro" />
        &nbsp;												fa-camera-retro
                  </p><p><i className="fa fa-key" />
        &nbsp;												fa-key
                  </p><p><i className="fa fa-gears" />
        &nbsp;												fa-gears
                  </p><p><i className="fa fa-cogs" />
        &nbsp;												fa-cogs
                  </p><p><i className="fa fa-comments" />
        &nbsp;												fa-comments
                  </p><p><i className="fa fa-thumbs-o-up" />
        &nbsp;												fa-thumbs-o-up
                  </p><p><i className="fa fa-thumbs-o-down" />
        &nbsp;												fa-thumbs-o-down
                  </p><p><i className="fa fa-star-half" />
        &nbsp;												fa-star-half
                  </p><p><i className="fa fa-heart-o" />
        &nbsp;												fa-heart-o
                  </p><p><i className="fa fa-sign-out" />
        &nbsp;												fa-sign-out
                  </p><p><i className="fa fa-linkedin-square" />
        &nbsp;												fa-linkedin-square
                  </p><p><i className="fa fa-thumb-tack" />
        &nbsp;												fa-thumb-tack
                  </p><p><i className="fa fa-external-link" />
        &nbsp;												fa-external-link
                  </p><p><i className="fa fa-sign-in" />
        &nbsp;												fa-sign-in
                  </p><p><i className="fa fa-trophy" />
        &nbsp;												fa-trophy
                  </p><p><i className="fa fa-github-square" />
        &nbsp;												fa-github-square
                  </p><p><i className="fa fa-upload" />
        &nbsp;												fa-upload
                  </p><p><i className="fa fa-lemon-o" />
        &nbsp;												fa-lemon-o
                  </p><p><i className="fa fa-phone" />
        &nbsp;												fa-phone
                  </p><p><i className="fa fa-square-o" />
        &nbsp;												fa-square-o
                  </p><p><i className="fa fa-bookmark-o" />
        &nbsp;												fa-bookmark-o
                  </p><p><i className="fa fa-phone-square" />
        &nbsp;												fa-phone-square
                  </p><p><i className="fa fa-twitter" />
        &nbsp;												fa-twitter
                  </p><p><i className="fa fa-facebook" />
        &nbsp;												fa-facebook
                  </p><p><i className="fa fa-github" />
        &nbsp;												fa-github
                  </p><p><i className="fa fa-unlock" />
        &nbsp;												fa-unlock
                  </p><p><i className="fa fa-credit-card" />
        &nbsp;												fa-credit-card
                  </p><p><i className="fa fa-rss" />
        &nbsp;												fa-rss
                  </p><p><i className="fa fa-hdd-o" />
        &nbsp;												fa-hdd-o
                  </p><p><i className="fa fa-bullhorn" />
        &nbsp;												fa-bullhorn
                  </p><p><i className="fa fa-bell" />
        &nbsp;												fa-bell
                  </p><p><i className="fa fa-certificate" />
        &nbsp;												fa-certificate
                  </p><p><i className="fa fa-hand-o-right" />
        &nbsp;												fa-hand-o-right
                  </p><p><i className="fa fa-hand-o-left" />
        &nbsp;												fa-hand-o-left
                  </p><p><i className="fa fa-hand-o-up" />
        &nbsp;												fa-hand-o-up
                  </p><p><i className="fa fa-hand-o-down" />
        &nbsp;												fa-hand-o-down
                  </p><p><i className="fa fa-arrow-circle-left" />
        &nbsp;												fa-arrow-circle-left
                  </p><p><i className="fa fa-arrow-circle-right" />
        &nbsp;												fa-arrow-circle-right
                  </p><p><i className="fa fa-arrow-circle-up" />
        &nbsp;												fa-arrow-circle-up
                  </p><p><i className="fa fa-arrow-circle-down" />
        &nbsp;												fa-arrow-circle-down
                  </p><p><i className="fa fa-globe" />
        &nbsp;												fa-globe
                  </p><p><i className="fa fa-wrench" />
        &nbsp;												fa-wrench
                  </p><p><i className="fa fa-tasks" />
        &nbsp;												fa-tasks
                  </p><p><i className="fa fa-filter" />
        &nbsp;												fa-filter
                  </p><p><i className="fa fa-brifiase" />
        &nbsp;												fa-brifiase
                  </p><p><i className="fa fa-arrows-alt" />
        &nbsp;												fa-arrows-alt
                  </p><p><i className="fa fa-group" />
        &nbsp;												fa-group
                  </p><p><i className="fa fa-users" />
        &nbsp;												fa-users
                  </p><p><i className="fa fa-chain" />
        &nbsp;												fa-chain
                  </p><p><i className="fa fa-link" />
        &nbsp;												fa-link
                  </p><p><i className="fa fa-cloud" />
        &nbsp;												fa-cloud
                  </p><p><i className="fa fa-flask" />
        &nbsp;												fa-flask
                  </p><p><i className="fa fa-cut" />
        &nbsp;												fa-cut
                  </p><p><i className="fa fa-scissors" />
        &nbsp;												fa-scissors
                  </p><p><i className="fa fa-copy" />
        &nbsp;												fa-copy
                  </p><p><i className="fa fa-files-o" />
        &nbsp;												fa-files-o
                  </p><p><i className="fa fa-paperclip" />
        &nbsp;												fa-paperclip
                  </p><p><i className="fa fa-save" />
        &nbsp;												fa-save
                  </p><p><i className="fa fa-floppy-o" />
        &nbsp;												fa-floppy-o
                  </p><p><i className="fa fa-square" />
        &nbsp;												fa-square
                  </p><p><i className="fa fa-navicon" />
        &nbsp;												fa-navicon
                  </p><p><i className="fa fa-reorder" />
        &nbsp;												fa-reorder
                  </p><p><i className="fa fa-bars" />
        &nbsp;												fa-bars
                  </p><p><i className="fa fa-list-ul" />
        &nbsp;												fa-list-ul
                  </p><p><i className="fa fa-list-ol" />
        &nbsp;												fa-list-ol
                  </p><p><i className="fa fa-strikethrough" />
        &nbsp;												fa-strikethrough
                  </p><p><i className="fa fa-underline" />
        &nbsp;												fa-underline
                  </p><p><i className="fa fa-table" />
        &nbsp;												fa-table
                  </p><p><i className="fa fa-magic" />
        &nbsp;												fa-magic
                  </p><p><i className="fa fa-truck" />
        &nbsp;												fa-truck
                  </p><p><i className="fa fa-pinterest" />
        &nbsp;												fa-pinterest
                  </p><p><i className="fa fa-pinterest-square" />
        &nbsp;												fa-pinterest-square
                  </p><p><i className="fa fa-google-plus-square" />
        &nbsp;												fa-google-plus-square
                  </p><p><i className="fa fa-google-plus" />
        &nbsp;												fa-google-plus
                  </p><p><i className="fa fa-money" />
        &nbsp;												fa-money
                  </p><p><i className="fa fa-caret-down" />
        &nbsp;												fa-caret-down
                  </p><p><i className="fa fa-caret-up" />
        &nbsp;												fa-caret-up
                  </p><p><i className="fa fa-caret-left" />
        &nbsp;												fa-caret-left
                  </p><p><i className="fa fa-caret-right" />
        &nbsp;												fa-caret-right
                  </p><p><i className="fa fa-columns" />
        &nbsp;												fa-columns
                  </p><p><i className="fa fa-unsorted" />
        &nbsp;												fa-unsorted
                  </p><p><i className="fa fa-sort" />
        &nbsp;												fa-sort
                  </p><p><i className="fa fa-sort-down" />
        &nbsp;												fa-sort-down
                  </p><p><i className="fa fa-sort-desc" />
        &nbsp;												fa-sort-desc
                  </p><p><i className="fa fa-sort-up" />
        &nbsp;												fa-sort-up
                  </p><p><i className="fa fa-sort-asc" />
        &nbsp;												fa-sort-asc
                  </p><p><i className="fa fa-envelope" />
        &nbsp;												fa-envelope
                  </p><p><i className="fa fa-linkedin" />
        &nbsp;												fa-linkedin
                  </p><p><i className="fa fa-rotate-left" />
        &nbsp;												fa-rotate-left
                  </p><p><i className="fa fa-undo" />
        &nbsp;												fa-undo
                  </p><p><i className="fa fa-legal" />
        &nbsp;												fa-legal
                  </p><p><i className="fa fa-gavel" />
        &nbsp;												fa-gavel
                  </p><p><i className="fa fa-dashboard" />
        &nbsp;												fa-dashboard
                  </p><p><i className="fa fa-tachometer" />
        &nbsp;												fa-tachometer
                  </p><p><i className="fa fa-comment-o" />
        &nbsp;												fa-comment-o
                  </p><p><i className="fa fa-comments-o" />
        &nbsp;												fa-comments-o
                  </p><p><i className="fa fa-flash" />
        &nbsp;												fa-flash
                  </p><p><i className="fa fa-bolt" />
        &nbsp;												fa-bolt
                  </p><p><i className="fa fa-sitemap" />
        &nbsp;												fa-sitemap
                  </p><p><i className="fa fa-mbreli" />
        &nbsp;												fa-umbreli
                  </p><p><i className="fa fa-paste" />
        &nbsp;												fa-paste
                  </p><p><i className="fa fa-clipboard" />
        &nbsp;												fa-clipboard
                  </p><p><i className="fa fa-lightbulb-o" />
        &nbsp;												fa-lightbulb-o
                  </p><p><i className="fa fa-exchange" />
        &nbsp;												fa-exchange
                  </p><p><i className="fa fa-cloud-download" />
        &nbsp;												fa-cloud-download
                  </p><p><i className="fa fa-cloud-upload" />
        &nbsp;												fa-cloud-upload
                  </p><p><i className="fa fa-user-md" />
        &nbsp;												fa-user-md
                  </p><p><i className="fa fa-stethoscope" />
        &nbsp;												fa-stethoscope
                  </p><p><i className="fa fa-suitcase" />
        &nbsp;												fa-suitcase
                  </p><p><i className="fa fa-bell-o" />
        &nbsp;												fa-bell-o
                  </p><p><i className="fa fa-coffee" />
        &nbsp;												fa-coffee
                  </p><p><i className="fa fa-cutlery" />
        &nbsp;												fa-cutlery
                  </p><p><i className="fa fa-file-text-o" />
        &nbsp;												fa-file-text-o
                  </p><p><i className="fa fa-building-o" />
        &nbsp;												fa-building-o
                  </p><p><i className="fa fa-hospital-o" />
        &nbsp;												fa-hospital-o
                  </p><p><i className="fa fa-ambulance" />
        &nbsp;												fa-ambulance
                  </p><p><i className="fa fa-medkit" />
        &nbsp;												fa-medkit
                  </p><p><i className="fa fa-fighter-jet" />
        &nbsp;												fa-fighter-jet
                  </p><p><i className="fa fa-beer" />
        &nbsp;												fa-beer
                  </p><p><i className="fa fa-h-square" />
        &nbsp;												fa-h-square
                  </p><p><i className="fa fa-plus-square" />
        &nbsp;												fa-plus-square
                  </p>
                </div>
                <div className="fa col-lg-3">
                  <p><i className="fa fa-angle-double-right" />
        &nbsp;												fa-angle-double-right
                  </p><p><i className="fa fa-angle-double-up" />
        &nbsp;												fa-angle-double-up
                  </p><p><i className="fa fa-angle-double-down" />
        &nbsp;												fa-angle-double-down
                  </p><p><i className="fa fa-angle-left" />
        &nbsp;												fa-angle-left
                  </p><p><i className="fa fa-angle-right" />
        &nbsp;												fa-angle-right
                  </p><p><i className="fa fa-angle-up" />
        &nbsp;												fa-angle-up
                  </p><p><i className="fa fa-angle-down" />
        &nbsp;												fa-angle-down
                  </p><p><i className="fa fa-desktop" />
        &nbsp;												fa-desktop
                  </p><p><i className="fa fa-laptop" />
        &nbsp;												fa-laptop
                  </p><p><i className="fa fa-tablet" />
        &nbsp;												fa-tablet
                  </p><p><i className="fa fa-mobile-phone" />
        &nbsp;												fa-mobile-phone
                  </p><p><i className="fa fa-mobile" />
        &nbsp;												fa-mobile
                  </p><p><i className="fa fa-circle-o" />
        &nbsp;												fa-circle-o
                  </p><p><i className="fa fa-quote-left" />
        &nbsp;												fa-quote-left
                  </p><p><i className="fa fa-quote-right" />
        &nbsp;												fa-quote-right
                  </p><p><i className="fa fa-spinner" />
        &nbsp;												fa-spinner
                  </p><p><i className="fa fa-circle" />
        &nbsp;												fa-circle
                  </p><p><i className="fa fa-mail-reply" />
        &nbsp;												fa-mail-reply
                  </p><p><i className="fa fa-reply" />
        &nbsp;												fa-reply
                  </p><p><i className="fa fa-github-alt" />
        &nbsp;												fa-github-alt
                  </p><p><i className="fa fa-folder-o" />
        &nbsp;												fa-folder-o
                  </p><p><i className="fa fa-folder-open-o" />
        &nbsp;												fa-folder-open-o
                  </p><p><i className="fa fa-smile-o" />
        &nbsp;												fa-smile-o
                  </p><p><i className="fa fa-frown-o" />
        &nbsp;												fa-frown-o
                  </p><p><i className="fa fa-meh-o" />
        &nbsp;												fa-meh-o
                  </p><p><i className="fa fa-gamepad" />
        &nbsp;												fa-gamepad
                  </p><p><i className="fa fa-keyboard-o" />
        &nbsp;												fa-keyboard-o
                  </p><p><i className="fa fa-flag-o" />
        &nbsp;												fa-flag-o
                  </p><p><i className="fa fa-flag-checkered" />
        &nbsp;												fa-flag-checkered
                  </p><p><i className="fa fa-terminal" />
        &nbsp;												fa-terminal
                  </p><p><i className="fa fa-code" />
        &nbsp;												fa-code
                  </p><p><i className="fa fa-mail-reply-all" />
        &nbsp;												fa-mail-reply-all
                  </p><p><i className="fa fa-reply-all" />
        &nbsp;												fa-reply-all
                  </p><p><i className="fa fa-star-half-empty" />
        &nbsp;												fa-star-half-empty
                  </p><p><i className="fa fa-star-half-full" />
        &nbsp;												fa-star-half-full
                  </p><p><i className="fa fa-star-half-o" />
        &nbsp;												fa-star-half-o
                  </p><p><i className="fa fa-location-arrow" />
        &nbsp;												fa-location-arrow
                  </p><p><i className="fa fa-crop" />
        &nbsp;												fa-crop
                  </p><p><i className="fa fa-code-fork" />
        &nbsp;												fa-code-fork
                  </p><p><i className="fa fa-unlink" />
        &nbsp;												fa-unlink
                  </p><p><i className="fa fa-chain-broei" />
        &nbsp;												fa-chain-broei
                  </p><p><i className="fa fa-question" />
        &nbsp;												fa-question
                  </p><p><i className="fa fa-info" />
        &nbsp;												fa-info
                  </p><p><i className="fa fa-exclamation" />
        &nbsp;												fa-exclamation
                  </p><p><i className="fa fa-superscript" />
        &nbsp;												fa-superscript
                  </p><p><i className="fa fa-subscript" />
        &nbsp;												fa-subscript
                  </p><p><i className="fa fa-eraser" />
        &nbsp;												fa-eraser
                  </p><p><i className="fa fa-puzzle-piece" />
        &nbsp;												fa-puzzle-piece
                  </p><p><i className="fa fa-microphone" />
        &nbsp;												fa-microphone
                  </p><p><i className="fa fa-microphone-slash" />
        &nbsp;												fa-microphone-slash
                  </p><p><i className="fa fa-shield" />
        &nbsp;												fa-shield
                  </p><p><i className="fa fa-calendar-o" />
        &nbsp;												fa-calendar-o
                  </p><p><i className="fa fa-fire-extinguisher" />
        &nbsp;												fa-fire-extinguisher
                  </p><p><i className="fa fa-rocket" />
        &nbsp;												fa-rocket
                  </p><p><i className="fa fa-maxcdn" />
        &nbsp;												fa-maxcdn
                  </p><p><i className="fa fa-chevron-circle-left" />
        &nbsp;												fa-chevron-circle-left
                  </p><p><i className="fa fa-chevron-circle-right" />
        &nbsp;												fa-chevron-circle-right
                  </p><p><i className="fa fa-chevron-circle-up" />
        &nbsp;												fa-chevron-circle-up
                  </p><p><i className="fa fa-chevron-circle-down" />
        &nbsp;												fa-chevron-circle-down
                  </p><p><i className="fa fa-html5" />
        &nbsp;												fa-html5
                  </p><p><i className="fa fa-css3" />
        &nbsp;												fa-css3
                  </p><p><i className="fa fa-anchor" />
        &nbsp;												fa-anchor
                  </p><p><i className="fa fa-unlock-alt" />
        &nbsp;												fa-unlock-alt
                  </p><p><i className="fa fa-bullseye" />
        &nbsp;												fa-bullseye
                  </p><p><i className="fa fa-ellipsis-h" />
        &nbsp;												fa-ellipsis-h
                  </p><p><i className="fa fa-ellipsis-v" />
        &nbsp;												fa-ellipsis-v
                  </p><p><i className="fa fa-rss-square" />
        &nbsp;												fa-rss-square
                  </p><p><i className="fa fa-play-circle" />
        &nbsp;												fa-play-circle
                  </p><p><i className="fa fa-ticket" />
        &nbsp;												fa-ticket
                  </p><p><i className="fa fa-minus-square" />
        &nbsp;												fa-minus-square
                  </p><p><i className="fa fa-minus-square-o" />
        &nbsp;												fa-minus-square-o
                  </p><p><i className="fa fa-level-up" />
        &nbsp;												fa-level-up
                  </p><p><i className="fa fa-level-down" />
        &nbsp;												fa-level-down
                  </p><p><i className="fa fa-check-square" />
        &nbsp;												fa-check-square
                  </p><p><i className="fa fa-pencil-square" />
        &nbsp;												fa-pencil-square
                  </p><p><i className="fa fa-external-link-square" />
        &nbsp;												fa-external-link-square
                  </p><p><i className="fa fa-share-square" />
        &nbsp;												fa-share-square
                  </p><p><i className="fa fa-compass" />
        &nbsp;												fa-compass
                  </p><p><i className="fa fa-toggle-down" />
        &nbsp;												fa-toggle-down
                  </p><p><i className="fa fa-caret-square-o-down" />
        &nbsp;												fa-caret-square-o-down
                  </p><p><i className="fa fa-toggle-up" />
        &nbsp;												fa-toggle-up
                  </p><p><i className="fa fa-caret-square-o-up" />
        &nbsp;												fa-caret-square-o-up
                  </p><p><i className="fa fa-toggle-right" />
        &nbsp;												fa-toggle-right
                  </p><p><i className="fa fa-caret-square-o-right" />
        &nbsp;												fa-caret-square-o-right
                  </p><p><i className="fa fa-euro" />
        &nbsp;												fa-euro
                  </p><p><i className="fa fa-eur" />
        &nbsp;												fa-eur
                  </p><p><i className="fa fa-gbp" />
        &nbsp;												fa-gbp
                  </p><p><i className="fa fa-dollar" />
        &nbsp;												fa-dollar
                  </p><p><i className="fa fa-usd" />
        &nbsp;												fa-usd
                  </p><p><i className="fa fa-rupee" />
        &nbsp;												fa-rupee
                  </p><p><i className="fa fa-inr" />
        &nbsp;												fa-inr
                  </p><p><i className="fa fa-cny" />
        &nbsp;												fa-cny
                  </p><p><i className="fa fa-rmb" />
        &nbsp;												fa-rmb
                  </p><p><i className="fa fa-yen" />
        &nbsp;												fa-yen
                  </p><p><i className="fa fa-jpy" />
        &nbsp;												fa-jpy
                  </p><p><i className="fa fa-ruble" />
        &nbsp;												fa-ruble
                  </p><p><i className="fa fa-rouble" />
        &nbsp;												fa-rouble
                  </p><p><i className="fa fa-rub" />
        &nbsp;												fa-rub
                  </p><p><i className="fa fa-won" />
        &nbsp;												fa-won
                  </p><p><i className="fa fa-krw" />
        &nbsp;												fa-krw
                  </p><p><i className="fa fa-bitcoin" />
        &nbsp;												fa-bitcoin
                  </p><p><i className="fa fa-btc" />
        &nbsp;												fa-btc
                  </p><p><i className="fa fa-file" />
        &nbsp;												fa-file
                  </p><p><i className="fa fa-file-text" />
        &nbsp;												fa-file-text
                  </p><p><i className="fa fa-sort-alpha-asc" />
        &nbsp;												fa-sort-alpha-asc
                  </p><p><i className="fa fa-sort-alpha-desc" />
        &nbsp;												fa-sort-alpha-desc
                  </p><p><i className="fa fa-sort-amount-asc" />
        &nbsp;												fa-sort-amount-asc
                  </p><p><i className="fa fa-sort-amount-desc" />
        &nbsp;												fa-sort-amount-desc
                  </p><p><i className="fa fa-sort-numeric-asc" />
        &nbsp;												fa-sort-numeric-asc
                  </p><p><i className="fa fa-sort-numeric-desc" />
        &nbsp;												fa-sort-numeric-desc
                  </p><p><i className="fa fa-thumbs-up" />
        &nbsp;												fa-thumbs-up
                  </p><p><i className="fa fa-thumbs-down" />
        &nbsp;												fa-thumbs-down
                  </p><p><i className="fa fa-youtube-square" />
        &nbsp;												fa-youtube-square
                  </p><p><i className="fa fa-youtube" />
        &nbsp;												fa-youtube
                  </p><p><i className="fa fa-xing" />
        &nbsp;												fa-xing
                  </p><p><i className="fa fa-xing-square" />
        &nbsp;												fa-xing-square
                  </p><p><i className="fa fa-youtube-play" />
        &nbsp;												fa-youtube-play
                  </p><p><i className="fa fa-dropbox" />
        &nbsp;												fa-dropbox
                  </p><p><i className="fa fa-stack-overflow" />
        &nbsp;												fa-stack-overflow
                  </p><p><i className="fa fa-instagram" />
        &nbsp;												fa-instagram
                  </p><p><i className="fa fa-flickr" />
        &nbsp;												fa-flickr
                  </p><p><i className="fa fa-adn" />
        &nbsp;												fa-adn
                  </p><p><i className="fa fa-bitbucket" />
        &nbsp;												fa-bitbucket
                  </p><p><i className="fa fa-bitbucket-square" />
        &nbsp;												fa-bitbucket-square
                  </p><p><i className="fa fa-tumblr" />
        &nbsp;												fa-tumblr
                  </p>
                </div>
                <div className="fa col-lg-3">
                  <p><i className="fa fa-long-arrow-down" />
        &nbsp;												fa-long-arrow-down
                  </p><p><i className="fa fa-long-arrow-up" />
        &nbsp;												fa-long-arrow-up
                  </p><p><i className="fa fa-long-arrow-left" />
        &nbsp;												fa-long-arrow-left
                  </p><p><i className="fa fa-long-arrow-right" />
        &nbsp;												fa-long-arrow-right
                  </p><p><i className="fa fa-apple" />
        &nbsp;												fa-apple
                  </p><p><i className="fa fa-windows" />
        &nbsp;												fa-windows
                  </p><p><i className="fa fa-android" />
        &nbsp;												fa-android
                  </p><p><i className="fa fa-linux" />
        &nbsp;												fa-linux
                  </p><p><i className="fa fa-dribbble" />
        &nbsp;												fa-dribbble
                  </p><p><i className="fa fa-skype" />
        &nbsp;												fa-skype
                  </p><p><i className="fa fa-foursquare" />
        &nbsp;												fa-foursquare
                  </p><p><i className="fa fa-trello" />
        &nbsp;												fa-trello
                  </p><p><i className="fa fa-female" />
        &nbsp;												fa-female
                  </p><p><i className="fa fa-male" />
        &nbsp;												fa-male
                  </p><p><i className="fa fa-gittip" />
        &nbsp;												fa-gittip
                  </p><p><i className="fa fa-sun-o" />
        &nbsp;												fa-sun-o
                  </p><p><i className="fa fa-moon-o" />
        &nbsp;												fa-moon-o
                  </p><p><i className="fa fa-archive" />
        &nbsp;												fa-archive
                  </p><p><i className="fa fa-bug" />
        &nbsp;												fa-bug
                  </p><p><i className="fa fa-vk" />
        &nbsp;												fa-vk
                  </p><p><i className="fa fa-weibo" />
        &nbsp;												fa-weibo
                  </p><p><i className="fa fa-renren" />
        &nbsp;												fa-renren
                  </p><p><i className="fa fa-pagelines" />
        &nbsp;												fa-pagelines
                  </p><p><i className="fa fa-stack-exchange" />
        &nbsp;												fa-stack-exchange
                  </p><p><i className="fa fa-arrow-circle-o-right" />
        &nbsp;												fa-arrow-circle-o-right
                  </p><p><i className="fa fa-arrow-circle-o-left" />
        &nbsp;												fa-arrow-circle-o-left
                  </p><p><i className="fa fa-toggle-left" />
        &nbsp;												fa-toggle-left
                  </p><p><i className="fa fa-caret-square-o-left" />
        &nbsp;												fa-caret-square-o-left
                  </p><p><i className="fa fa-dot-circle-o" />
        &nbsp;												fa-dot-circle-o
                  </p><p><i className="fa fa-wheelchair" />
        &nbsp;												fa-wheelchair
                  </p><p><i className="fa fa-vimeo-square" />
        &nbsp;												fa-vimeo-square
                  </p><p><i className="fa fa-turkish-lira" />
        &nbsp;												fa-turkish-lira
                  </p><p><i className="fa fa-try" />
        &nbsp;												fa-try
                  </p><p><i className="fa fa-plus-square-o" />
        &nbsp;												fa-plus-square-o
                  </p><p><i className="fa fa-space-shuttle" />
        &nbsp;												fa-space-shuttle
                  </p><p><i className="fa fa-slack" />
        &nbsp;												fa-slack
                  </p><p><i className="fa fa-envelope-square" />
        &nbsp;												fa-envelope-square
                  </p><p><i className="fa fa-wordpress" />
        &nbsp;												fa-wordpress
                  </p><p><i className="fa fa-openid" />
        &nbsp;												fa-openid
                  </p><p><i className="fa fa-institution" />
        &nbsp;												fa-institution
                  </p><p><i className="fa fa-bank" />
        &nbsp;												fa-bank
                  </p><p><i className="fa fa-university" />
        &nbsp;												fa-university
                  </p><p><i className="fa fa-mortar-board" />
        &nbsp;												fa-mortar-board
                  </p><p><i className="fa fa-graduation-cap" />
        &nbsp;												fa-graduation-cap
                  </p><p><i className="fa fa-yahoo" />
        &nbsp;												fa-yahoo
                  </p><p><i className="fa fa-google" />
        &nbsp;												fa-google
                  </p><p><i className="fa fa-reddit" />
        &nbsp;												fa-reddit
                  </p><p><i className="fa fa-reddit-square" />
        &nbsp;												fa-reddit-square
                  </p><p><i className="fa fa-stumbleupon-circle" />
        &nbsp;												fa-stumbleupon-circle
                  </p><p><i className="fa fa-stumbleupon" />
        &nbsp;												fa-stumbleupon
                  </p><p><i className="fa fa-delicious" />
        &nbsp;												fa-delicious
                  </p><p><i className="fa fa-digg" />
        &nbsp;												fa-digg
                  </p><p><i className="fa fa-pied-piper-square" />
        &nbsp;												fa-pied-piper-square
                  </p><p><i className="fa fa-pied-piper" />
        &nbsp;												fa-pied-piper
                  </p><p><i className="fa fa-pied-piper-alt" />
        &nbsp;												fa-pied-piper-alt
                  </p><p><i className="fa fa-drupal" />
        &nbsp;												fa-drupal
                  </p><p><i className="fa fa-joomla" />
        &nbsp;												fa-joomla
                  </p><p><i className="fa fa-language" />
        &nbsp;												fa-language
                  </p><p><i className="fa fa-fax" />
        &nbsp;												fa-fax
                  </p><p><i className="fa fa-building" />
        &nbsp;												fa-building
                  </p><p><i className="fa fa-child" />
        &nbsp;												fa-child
                  </p><p><i className="fa fa-paw" />
        &nbsp;												fa-paw
                  </p><p><i className="fa fa-spoon" />
        &nbsp;												fa-spoon
                  </p><p><i className="fa fa-cube" />
        &nbsp;												fa-cube
                  </p><p><i className="fa fa-cubes" />
        &nbsp;												fa-cubes
                  </p><p><i className="fa fa-behance" />
        &nbsp;												fa-behance
                  </p><p><i className="fa fa-behance-square" />
        &nbsp;												fa-behance-square
                  </p><p><i className="fa fa-steam" />
        &nbsp;												fa-steam
                  </p><p><i className="fa fa-steam-square" />
        &nbsp;												fa-steam-square
                  </p><p><i className="fa fa-recycle" />
        &nbsp;												fa-recycle
                  </p><p><i className="fa fa-automobile" />
        &nbsp;												fa-automobile
                  </p><p><i className="fa fa-car" />
        &nbsp;												fa-car
                  </p><p><i className="fa fa-cab" />
        &nbsp;												fa-cab
                  </p><p><i className="fa fa-taxi" />
        &nbsp;												fa-taxi
                  </p><p><i className="fa fa-tree" />
        &nbsp;												fa-tree
                  </p><p><i className="fa fa-spotify" />
        &nbsp;												fa-spotify
                  </p><p><i className="fa fa-deviantart" />
        &nbsp;												fa-deviantart
                  </p><p><i className="fa fa-soundcloud" />
        &nbsp;												fa-soundcloud
                  </p><p><i className="fa fa-database" />
        &nbsp;												fa-database
                  </p><p><i className="fa fa-file-pdf-o" />
        &nbsp;												fa-file-pdf-o
                  </p><p><i className="fa fa-file-word-o" />
        &nbsp;												fa-file-word-o
                  </p><p><i className="fa fa-file-excel-o" />
        &nbsp;												fa-file-excel-o
                  </p><p><i className="fa fa-file-powerpoint-o" />
        &nbsp;												fa-file-powerpoint-o
                  </p><p><i className="fa fa-file-photo-o" />
        &nbsp;												fa-file-photo-o
                  </p><p><i className="fa fa-file-picture-o" />
        &nbsp;												fa-file-picture-o
                  </p><p><i className="fa fa-file-image-o" />
        &nbsp;												fa-file-image-o
                  </p><p><i className="fa fa-file-zip-o" />
        &nbsp;												fa-file-zip-o
                  </p><p><i className="fa fa-file-archive-o" />
        &nbsp;												fa-file-archive-o
                  </p><p><i className="fa fa-file-sound-o" />
        &nbsp;												fa-file-sound-o
                  </p><p><i className="fa fa-file-audio-o" />
        &nbsp;												fa-file-audio-o
                  </p><p><i className="fa fa-file-movie-o" />
        &nbsp;												fa-file-movie-o
                  </p><p><i className="fa fa-file-video-o" />
        &nbsp;												fa-file-video-o
                  </p><p><i className="fa fa-file-code-o" />
        &nbsp;												fa-file-code-o
                  </p><p><i className="fa fa-vine" />
        &nbsp;												fa-vine
                  </p><p><i className="fa fa-codepen" />
        &nbsp;												fa-codepen
                  </p><p><i className="fa fa-jsfiddle" />
        &nbsp;												fa-jsfiddle
                  </p><p><i className="fa fa-life-bouy" />
        &nbsp;												fa-life-bouy
                  </p><p><i className="fa fa-life-saver" />
        &nbsp;												fa-life-saver
                  </p><p><i className="fa fa-support" />
        &nbsp;												fa-support
                  </p><p><i className="fa fa-life-ring" />
        &nbsp;												fa-life-ring
                  </p><p><i className="fa fa-circle-o-notch" />
        &nbsp;												fa-circle-o-notch
                  </p><p><i className="fa fa-ra" />
        &nbsp;												fa-ra
                  </p><p><i className="fa fa-rebel" />
        &nbsp;												fa-rebel
                  </p><p><i className="fa fa-ge" />
        &nbsp;												fa-ge
                  </p><p><i className="fa fa-empire" />
        &nbsp;												fa-empire
                  </p><p><i className="fa fa-git-square" />
        &nbsp;												fa-git-square
                  </p><p><i className="fa fa-git" />
        &nbsp;												fa-git
                  </p><p><i className="fa fa-hacker-news" />
        &nbsp;												fa-hacker-news
                  </p><p><i className="fa fa-tencent-weibo" />
        &nbsp;												fa-tencent-weibo
                  </p><p><i className="fa fa-qq" />
        &nbsp;												fa-qq
                  </p><p><i className="fa fa-wechat" />
        &nbsp;												fa-wechat
                  </p><p><i className="fa fa-weixin" />
        &nbsp;												fa-weixin
                  </p><p><i className="fa fa-send" />
        &nbsp;												fa-send
                  </p><p><i className="fa fa-paper-plane" />
        &nbsp;												fa-paper-plane
                  </p><p><i className="fa fa-send-o" />
        &nbsp;												fa-send-o
                  </p><p><i className="fa fa-paper-plane-o" />
        &nbsp;												fa-paper-plane-o
                  </p><p><i className="fa fa-history" />
        &nbsp;												fa-history
                  </p><p><i className="fa fa-circle-thin" />
        &nbsp;												fa-circle-thin
                  </p><p><i className="fa fa-header" />
        &nbsp;												fa-header
                  </p><p><i className="fa fa-paragraph" />
        &nbsp;												fa-paragraph
                  </p><p><i className="fa fa-sliders" />
        &nbsp;												fa-sliders
                  </p><p><i className="fa fa-share-alt" />
        &nbsp;												fa-share-alt
                  </p><p><i className="fa fa-share-alt-square" />
        &nbsp;												fa-share-alt-square
                  </p><p><i className="fa fa-bomb" />
        &nbsp;												fa-bomb
                  </p>
                </div>
              </div>
            </Panel>
          </div>
        </div>
      </div>
    );
  }
}
export default Icons;
