import React, { Component, PropTypes } from 'react';
import emptyFunction from 'fbjs/lib/emptyFunction';
import { IntlProvider, addLocaleData } from 'react-intl';
import $ from 'jquery';
import s from './App.css';
import style from '../../common/styles/bootstrap.scss';
import Header from '../Header';
import Sidebar from '../Sidebar';
import dutchMessages from '../../../languages/de.json';
import englishMessages from '../../../languages/en.json';
import hindiMessages from '../../../languages/hn.json';
import urduMessages from '../../../languages/ur.json';
import en from 'react-intl/locale-data/en';
import hi from 'react-intl/locale-data/hi';
import ur from 'react-intl/locale-data/ur';
import de from 'react-intl/locale-data/de';

// import localForage from 'localforage';

addLocaleData([...en, ...hi, ...ur, ...de]);

const langMessage = {
  'en': englishMessages,
  'hi': hindiMessages,
  'de': dutchMessages,
  'ur': urduMessages,
}


class App extends Component {

  static propTypes = {
    context: PropTypes.shape({
      insertCss: PropTypes.func,
      setTitle: PropTypes.func,
      setMeta: PropTypes.func,
      setLang: PropTypes.func,
    }),
    children: PropTypes.element.isRequired,
    error: PropTypes.object,
  };

  static childContextTypes = {
    insertCss: PropTypes.func.isRequired,
    setTitle: PropTypes.func.isRequired,
    setMeta: PropTypes.func.isRequired,
    setLang: PropTypes.func,
  };

  constructor(props) {
    super(props);
    // console.log('local lang in app js', localForage.getItem('lang',(err, value) => {
    //   console.log('value of lang 123', value);
    // }));
    this.state = {
      lang: 'en',
      message: englishMessages,
    };
  }

  getChildContext() {
    const context = this.props.context;
    return {
      insertCss: context.insertCss || emptyFunction,
      setTitle: context.setTitle || emptyFunction,
      setMeta: context.setMeta || emptyFunction,
      setLang: lang => {
        let textMessage = langMessage[lang];
        this.setState({
          lang,
          message: textMessage,
        });
      },
    };
  }

  componentWillMount() {
    // console.log("fdsfsd", $(window));
    const { insertCss } = this.props.context;
    this.removeCss = insertCss(s);
    insertCss(style);
    // localForage.getItem('lang', (err, value) => {
    //   if (value) {
    //     // console.log('lang messagesssssssss', langMessage[value]);
    //     this.setState({
    //       lang: value,
    //       message: langMessage[value],
    //     });
    //   }
    // })
  }

  componentDidMount() {
    // console.log("local storage", window.localStorage.getItem("language123"));
    window.scrollTo(0,0);
    let setLang = window.localStorage.getItem("language123");
    if (!setLang) {
        setLang = 'en';
    }
    this.setState({
      lang: setLang,
      message: langMessage[setLang],
    });
  }

  componentWillUpdate() {

    let ele =  document.querySelector('section');
    if (ele) {
      ele.scrollTop = 0;
    }
  }

  componentWillUnmount() {
    this.removeCss();
  }


  render() {
    // console.log('inside render', this.state);
    return (!this.props.error && this.props.header) ? (
            <IntlProvider locale={this.state.lang} messages={this.state.message}>
              <div className={`dashboard-page ${s.dashboardPage}`}>
                <Header />
                <Sidebar />
                <section id={s.bodyContainer} className={s.uiView}>
                  {this.props.children}
                </section>
              </div>
          </IntlProvider>
    ) :(
        this.props.children
    );
  }

}

// export default withStyles(s)(App);
export default App;


// <ReactCSSTransitionGroup
//   transitionName="ani"
//   transitionAppear={true}
//   transitionAppearTimeout={500}
//   transitionEnter={false}
//   transitionLeave={false}
//   >
//   <section id={s.bodyContainer} className={s.uiView}>
//     {this.props.children}
//   </section>
// </ReactCSSTransitionGroup>
